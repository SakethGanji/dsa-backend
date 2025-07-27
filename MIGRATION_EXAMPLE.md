# API → Service Migration Example

## Example: Dataset Feature Migration

### Before: Multiple Handlers
```python
# 8 different handler files:
- CreateDatasetHandler
- GetDatasetHandler  
- ListDatasetsHandler
- UpdateDatasetHandler
- DeleteDatasetHandler
- GrantPermissionHandler
- CheckDatasetReadyHandler
- CreateDatasetWithFileHandler
```

### After: Single DatasetService
```python
# src/features/datasets/services.py
from src.features.base_service import BaseService
from src.core.domain_exceptions import EntityNotFoundException, ForbiddenException
from typing import List, Tuple, Optional

class DatasetService(BaseService):
    """All dataset-related business logic in one place"""
    
    # CREATE operations
    async def create_dataset(self, name: str, description: str, user_id: int, 
                           tags: List[str] = None) -> CreateDatasetResponse:
        """Create a new dataset"""
        async with self._uow:
            # Business logic from CreateDatasetHandler
            dataset_id = await self._uow.datasets.create_dataset(
                name=name, description=description, created_by=user_id
            )
            
            # Grant admin permission
            await self._uow.datasets.grant_permission(
                dataset_id=dataset_id, user_id=user_id, permission_type='admin'
            )
            
            # Create initial commit
            commit_id = await self._uow.commits.create_initial_commit(
                dataset_id=dataset_id, author_id=user_id
            )
            
            # Publish event
            await self._publish_event(DatasetCreatedEvent(
                dataset_id=dataset_id, user_id=user_id, name=name
            ))
            
            await self._uow.commit()
            
            dataset = await self._uow.datasets.get_dataset_by_id(dataset_id)
            return CreateDatasetResponse(**dataset)
    
    async def create_dataset_with_file(self, name: str, file_path: str, 
                                     user_id: int, **kwargs) -> CreateDatasetResponse:
        """Create dataset and import file in one operation"""
        # Combines CreateDatasetHandler + import logic
        dataset = await self.create_dataset(name, kwargs.get('description', ''), user_id)
        
        # Queue import job
        job_id = await self._uow.jobs.create_job(
            run_type='import',
            dataset_id=dataset.id,
            user_id=user_id,
            run_parameters={'file_path': file_path}
        )
        
        return CreateDatasetWithFileResponse(
            dataset_id=dataset.id,
            import_job_id=job_id
        )
    
    # READ operations  
    async def get_dataset(self, dataset_id: int, user_id: int) -> DatasetDetailResponse:
        """Get dataset details"""
        # Check permission
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        dataset = await self._uow.datasets.get_dataset_by_id(dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", dataset_id)
            
        tags = await self._uow.datasets.get_dataset_tags(dataset_id)
        
        return DatasetDetailResponse(**dataset, tags=tags)
    
    async def list_datasets(self, user_id: int, offset: int = 0, 
                          limit: int = 20, **filters) -> Tuple[List[DatasetSummary], int]:
        """List datasets user has access to"""
        # Validate pagination
        offset = max(0, offset)
        limit = min(100, max(1, limit))
        
        datasets, total = await self._uow.datasets.list_user_datasets(
            user_id=user_id,
            offset=offset,
            limit=limit,
            **filters
        )
        
        return [DatasetSummary(**d) for d in datasets], total
    
    # UPDATE operations
    async def update_dataset(self, dataset_id: int, user_id: int, 
                           **updates) -> DatasetDetailResponse:
        """Update dataset metadata"""
        async with self._uow:
            # Check permission
            await self._permissions.require("dataset", dataset_id, user_id, "write")
            
            # Update
            await self._uow.datasets.update_dataset(dataset_id, **updates)
            
            # Publish event
            await self._publish_event(DatasetUpdatedEvent(
                dataset_id=dataset_id, user_id=user_id, updates=updates
            ))
            
            await self._uow.commit()
            
            return await self.get_dataset(dataset_id, user_id)
    
    # DELETE operations
    async def delete_dataset(self, dataset_id: int, user_id: int) -> None:
        """Delete a dataset"""
        async with self._uow:
            # Check permission - only admin can delete
            await self._permissions.require("dataset", dataset_id, user_id, "admin")
            
            # Soft delete
            await self._uow.datasets.mark_deleted(dataset_id)
            
            await self._publish_event(DatasetDeletedEvent(
                dataset_id=dataset_id, user_id=user_id
            ))
            
            await self._uow.commit()
    
    # PERMISSION operations
    async def grant_permission(self, dataset_id: int, granting_user_id: int,
                             target_user_id: int, permission_type: str) -> None:
        """Grant permission to another user"""
        async with self._uow:
            # Check if granting user is admin
            await self._permissions.require("dataset", dataset_id, granting_user_id, "admin")
            
            # Grant permission
            await self._uow.datasets.grant_permission(
                dataset_id=dataset_id,
                user_id=target_user_id,
                permission_type=permission_type
            )
            
            await self._uow.commit()
    
    # STATUS operations
    async def check_dataset_ready(self, dataset_id: int, user_id: int) -> DatasetReadyStatus:
        """Check if dataset has data and is ready for use"""
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Check if dataset has any commits with data
        latest_commit = await self._uow.commits.get_latest_commit(dataset_id, 'main')
        has_data = latest_commit and latest_commit.get('row_count', 0) > 0
        
        return DatasetReadyStatus(
            dataset_id=dataset_id,
            is_ready=has_data,
            message="Dataset has data" if has_data else "Dataset is empty"
        )
```

## API Layer Becomes Simple:

```python
# src/api/datasets.py
from fastapi import APIRouter, Depends
from src.features.datasets.services import DatasetService
from src.api.dependencies import get_dataset_service, get_current_user_id

router = APIRouter(prefix="/datasets", tags=["datasets"])

# CREATE
@router.post("/", response_model=CreateDatasetResponse)
async def create_dataset(
    request: CreateDatasetRequest,
    user_id: int = Depends(get_current_user_id),
    service: DatasetService = Depends(get_dataset_service)
):
    return await service.create_dataset(
        name=request.name,
        description=request.description,
        user_id=user_id,
        tags=request.tags
    )

# READ
@router.get("/{dataset_id}", response_model=DatasetDetailResponse)
async def get_dataset(
    dataset_id: int,
    user_id: int = Depends(get_current_user_id),
    service: DatasetService = Depends(get_dataset_service)
):
    return await service.get_dataset(dataset_id, user_id)

@router.get("/", response_model=DatasetListResponse)
async def list_datasets(
    offset: int = 0,
    limit: int = 20,
    search: Optional[str] = None,
    user_id: int = Depends(get_current_user_id),
    service: DatasetService = Depends(get_dataset_service)
):
    datasets, total = await service.list_datasets(
        user_id=user_id,
        offset=offset,
        limit=limit,
        search=search
    )
    return DatasetListResponse(datasets=datasets, total=total, offset=offset, limit=limit)

# UPDATE
@router.patch("/{dataset_id}", response_model=DatasetDetailResponse)
async def update_dataset(
    dataset_id: int,
    request: UpdateDatasetRequest,
    user_id: int = Depends(get_current_user_id),
    service: DatasetService = Depends(get_dataset_service)
):
    return await service.update_dataset(
        dataset_id=dataset_id,
        user_id=user_id,
        **request.dict(exclude_unset=True)
    )

# DELETE
@router.delete("/{dataset_id}", status_code=204)
async def delete_dataset(
    dataset_id: int,
    user_id: int = Depends(get_current_user_id),
    service: DatasetService = Depends(get_dataset_service)
):
    await service.delete_dataset(dataset_id, user_id)

# PERMISSIONS
@router.post("/{dataset_id}/permissions", status_code=201)
async def grant_permission(
    dataset_id: int,
    request: GrantPermissionRequest,
    user_id: int = Depends(get_current_user_id),
    service: DatasetService = Depends(get_dataset_service)
):
    await service.grant_permission(
        dataset_id=dataset_id,
        granting_user_id=user_id,
        target_user_id=request.user_id,
        permission_type=request.permission_type
    )
```

## Benefits of This Approach:

1. **Single source of truth**: All dataset logic in DatasetService
2. **Easier to find code**: Looking for dataset operations? Check DatasetService
3. **Better cohesion**: Related operations are together
4. **Simpler testing**: Mock one service instead of multiple handlers
5. **Clean API layer**: Just parameter extraction and response formatting
6. **Reusable**: Same service can be used by workers, CLI tools, etc.

## Service Organization Guidelines:

### Feature Services (Business Logic)
- `DatasetService`: All dataset CRUD + permissions
- `UserService`: User management, authentication
- `JobService`: Job creation, status, cancellation
- `VersioningService`: Commits, branches, refs
- `ExplorationService`: Data profiling
- `SamplingService`: Statistical sampling
- `SearchService`: Search operations
- `SqlWorkbenchService`: SQL execution

### Utility Services (Shared Functionality)
Keep these in `/src/services/`:
- `FileProcessingService`: Parse CSV, Excel, Parquet
- `DataExportService`: Export to various formats
- `CommitPreparationService`: Prepare commit data
- `TableAnalysisService`: Analyze table statistics

The utility services are called BY feature services, not directly from API.