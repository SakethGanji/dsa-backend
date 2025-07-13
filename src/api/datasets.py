"""Dataset management API endpoints."""

from fastapi import APIRouter, Depends, status, Path, UploadFile, File, Form
from typing import Annotated, AsyncGenerator, Dict, Any
from ..infrastructure.postgres.database import DatabasePool, UnitOfWorkFactory
from ..infrastructure.postgres import PostgresDatasetRepository
from ..features.datasets.grant_permission import GrantPermissionHandler
from ..models.pydantic_models import (
    CreateDatasetRequest, CreateDatasetResponse,
    GrantPermissionRequest, GrantPermissionResponse,
    CurrentUser, ListDatasetsResponse, DatasetSummary,
    DatasetDetailResponse, UpdateDatasetRequest,
    UpdateDatasetResponse, DeleteDatasetResponse,
    CreateDatasetWithFileRequest, CreateDatasetWithFileResponse
)
from ..core.authorization import (
    get_current_user_info,
    require_dataset_admin,
    require_dataset_read,
    require_dataset_write,
    PermissionType
)
from ..core.exceptions import resource_not_found
from ..core.domain_exceptions import ConflictException
from .dependencies import get_db_pool


router = APIRouter(prefix="/datasets", tags=["datasets"])


# Local dependency helpers
async def get_uow_factory(
    pool: DatabasePool = Depends(get_db_pool)
) -> UnitOfWorkFactory:
    """Get unit of work factory."""
    return UnitOfWorkFactory(pool)


async def get_dataset_repo(
    pool: DatabasePool = Depends(get_db_pool)
) -> AsyncGenerator[PostgresDatasetRepository, None]:
    """Get dataset repository."""
    async with pool.acquire() as conn:
        yield PostgresDatasetRepository(conn)


@router.post("/", response_model=CreateDatasetResponse)
async def create_dataset(
    request: CreateDatasetRequest,
    current_user: CurrentUser = Depends(get_current_user_info),
    pool: DatabasePool = Depends(get_db_pool)
) -> CreateDatasetResponse:
    """Create a new dataset with optional tags."""
    async with pool.acquire() as conn:
        dataset_repo = PostgresDatasetRepository(conn)
        
        # Check if dataset with same name already exists for this user
        existing = await dataset_repo.get_dataset_by_name_and_user(
            name=request.name,
            user_id=current_user.user_id
        )
        if existing:
            raise ConflictException(
                f"Dataset with name '{request.name}' already exists",
                conflicting_field="name",
                existing_value=request.name
            )
        
        # Create the dataset
        dataset_id = await dataset_repo.create_dataset(
            name=request.name,
            description=request.description or "",
            created_by=current_user.user_id
        )
        
        # Add tags if provided
        if request.tags:
            await dataset_repo.add_dataset_tags(dataset_id, request.tags)
        
        # Get the tags back to include in response
        tags = await dataset_repo.get_dataset_tags(dataset_id)
        
        # Refresh search index to include new dataset
        from ..infrastructure.postgres.search_repository import PostgresSearchRepository
        search_repo = PostgresSearchRepository(conn)
        await search_repo.refresh_search_index()
        
        return CreateDatasetResponse(
            dataset_id=dataset_id,
            name=request.name,
            description=request.description,
            tags=tags
        )


@router.post("/create-with-file", response_model=CreateDatasetWithFileResponse)
async def create_dataset_with_file(
    name: str = Form(...),
    file: UploadFile = File(...),
    description: str = Form(None),
    tags: str = Form(None),  # comma-separated tags
    default_branch: str = Form("main"),
    commit_message: str = Form("Initial import"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    pool: DatabasePool = Depends(get_db_pool)
) -> CreateDatasetWithFileResponse:
    """Create a new dataset and import a file in one operation."""
    # Parse tags from comma-separated string
    tag_list = [tag.strip() for tag in tags.split(",")] if tags else []
    
    # Create request object
    request = CreateDatasetWithFileRequest(
        name=name,
        description=description,
        tags=tag_list,
        default_branch=default_branch,
        commit_message=commit_message
    )
    
    # Implement the logic directly without handlers to avoid connection issues
    async with pool.acquire() as conn:
        from ..infrastructure.postgres import (
            PostgresDatasetRepository, 
            PostgresCommitRepository,
            PostgresJobRepository
        )
        
        # Start transaction
        async with conn.transaction():
            dataset_repo = PostgresDatasetRepository(conn)
            commit_repo = PostgresCommitRepository(conn)
            job_repo = PostgresJobRepository(conn)
            
            # Check if dataset with same name already exists
            existing = await dataset_repo.get_dataset_by_name_and_user(
                name=request.name,
                user_id=current_user.user_id
            )
            if existing:
                raise ConflictException(
                f"Dataset with name '{request.name}' already exists",
                conflicting_field="name",
                existing_value=request.name
            )
            
            # Create the dataset
            dataset_id = await dataset_repo.create_dataset(
                name=request.name,
                description=request.description or "",
                created_by=current_user.user_id
            )
            
            # Grant admin permission to creator
            await dataset_repo.grant_permission(
                dataset_id=dataset_id,
                user_id=current_user.user_id,
                permission_type='admin'
            )
            
            # Add tags if provided
            if request.tags:
                await dataset_repo.add_dataset_tags(dataset_id, request.tags)
            
            # Create initial empty commit
            initial_commit_id = await commit_repo.create_commit_and_manifest(
                dataset_id=dataset_id,
                parent_commit_id=None,
                message="Initial commit",
                author_id=current_user.user_id,
                manifest=[]  # Empty manifest for initial commit
            )
            
            # Update the default branch ref (it was created with NULL commit_id)
            # The create_dataset method creates a 'main' ref with NULL commit_id
            if request.default_branch == "main":
                # Update existing ref from NULL to the initial commit
                await commit_repo.update_ref_atomically(
                    dataset_id=dataset_id,
                    ref_name=request.default_branch,
                    expected_commit_id=None,  # Current value is NULL
                    new_commit_id=initial_commit_id
                )
            else:
                # Create new ref for non-main branches
                await commit_repo.create_ref(
                    dataset_id=dataset_id,
                    ref_name=request.default_branch,
                    commit_id=initial_commit_id
                )
            
            # Save uploaded file to temporary location
            import tempfile
            import os
            temp_dir = tempfile.gettempdir()
            temp_path = os.path.join(temp_dir, f"import_{dataset_id}_{file.filename}")
            
            # Read and save file content
            file_content = await file.read()
            with open(temp_path, 'wb') as f:
                f.write(file_content)
            
            # Create import job
            job_parameters = {
                "temp_file_path": temp_path,
                "filename": file.filename,
                "commit_message": request.commit_message,
                "target_ref": request.default_branch
            }
            
            job_id = await job_repo.create_job(
                run_type='import',
                dataset_id=dataset_id,
                user_id=current_user.user_id,
                source_commit_id=initial_commit_id,
                run_parameters=job_parameters
            )
            
            # Get the tags for response
            tags = await dataset_repo.get_dataset_tags(dataset_id)
            
            # Refresh search index
            from ..infrastructure.postgres.search_repository import PostgresSearchRepository
            search_repo = PostgresSearchRepository(conn)
            await search_repo.refresh_search_index()
            
            return CreateDatasetWithFileResponse(
                dataset_id=dataset_id,
                name=request.name,
                description=request.description,
                tags=tags,
                import_job_id=job_id,
                status="pending",
                message="Dataset created and import job queued successfully"
            )


@router.post("/{dataset_id}/permissions", response_model=GrantPermissionResponse)
async def grant_permission(
    dataset_id: int = Path(..., description="Dataset ID"),
    request: GrantPermissionRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    dataset_repo: PostgresDatasetRepository = Depends(get_dataset_repo),
    _: CurrentUser = Depends(require_dataset_admin)
) -> GrantPermissionResponse:
    """Grant permission to a user on a dataset (admin only)."""
    uow = uow_factory.create()
    handler = GrantPermissionHandler(uow, dataset_repo)
    
    # Handler expects granting_user_id parameter
    return await handler.handle(dataset_id, request, current_user.user_id)


@router.get("/", response_model=ListDatasetsResponse)
async def list_datasets(
    offset: int = 0,
    limit: int = 100,
    current_user: CurrentUser = Depends(get_current_user_info),
    dataset_repo: PostgresDatasetRepository = Depends(get_dataset_repo),
    pool: DatabasePool = Depends(get_db_pool)
) -> ListDatasetsResponse:
    """List all datasets accessible to the current user with pagination."""
    # Get all datasets for the user
    all_datasets = await dataset_repo.list_user_datasets(current_user.user_id)
    
    # Apply pagination
    total = len(all_datasets)
    paginated_datasets = all_datasets[offset:offset + limit]
    
    # Get job repository to check import status
    from ..infrastructure.postgres import PostgresJobRepository
    
    # Convert to response format with tags and import status
    dataset_summaries = []
    async with pool.acquire() as conn:
        job_repo = PostgresJobRepository(conn)
        dataset_repo_new = PostgresDatasetRepository(conn)
        
        for dataset in paginated_datasets:
            tags = await dataset_repo_new.get_dataset_tags(dataset['dataset_id'])
            
            # Check for latest import job
            import_status = None
            import_job_id = None
            # Get import job but don't fail if it doesn't exist
            latest_import_job = await job_repo.get_latest_import_job(dataset['dataset_id'])
            if latest_import_job:
                import_job_id = str(latest_import_job['job_id'])
                import_status = latest_import_job['status']
            
            dataset_summaries.append(DatasetSummary(
                dataset_id=dataset['dataset_id'],
                name=dataset['name'],
                description=dataset['description'],
                created_by=dataset['created_by'],
                created_at=dataset['created_at'],
                updated_at=dataset['updated_at'],
                permission_type=dataset['permission_type'],
                tags=tags,
                import_status=import_status,
                import_job_id=import_job_id
            ))
    
    return ListDatasetsResponse(
        datasets=dataset_summaries,
        total=total,
        offset=offset,
        limit=limit
    )


@router.get("/{dataset_id}", response_model=DatasetDetailResponse)
async def get_dataset(
    dataset_id: int = Path(..., description="Dataset ID"),
    current_user: CurrentUser = Depends(get_current_user_info),
    pool: DatabasePool = Depends(get_db_pool)
) -> DatasetDetailResponse:
    """Get detailed information about a specific dataset."""
    
    # Get fresh connection for this request
    async with pool.acquire() as conn:
        dataset_repo = PostgresDatasetRepository(conn)
        
        # Get dataset details
        dataset = await dataset_repo.get_dataset_by_id(dataset_id)
        if not dataset:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Dataset {dataset_id} not found in database")
            raise resource_not_found("Dataset", dataset_id)
        
        # Check permission
        has_permission = await dataset_repo.check_user_permission(
            dataset_id=dataset_id,
            user_id=current_user.user_id,
            required_permission=PermissionType.READ.value
        )
        if not has_permission:
            raise permission_denied("Dataset", PermissionType.READ.value)
        
        # Get tags
        tags = await dataset_repo.get_dataset_tags(dataset_id)
        
        # Get user's permission type for this dataset
        user_datasets = await dataset_repo.list_user_datasets(current_user.user_id)
        permission_type = None
        for ds in user_datasets:
            if ds['dataset_id'] == dataset_id:
                permission_type = ds['permission_type']
                break
        
        # Check for import job status
        import_status = None
        import_job_id = None
        from ..infrastructure.postgres import PostgresJobRepository
        job_repo = PostgresJobRepository(conn)
        latest_import_job = await job_repo.get_latest_import_job(dataset_id)
        if latest_import_job:
            import_job_id = str(latest_import_job['job_id'])
            import_status = latest_import_job['status']
        
        return DatasetDetailResponse(
            dataset_id=dataset['id'],  # Changed from dataset_id to id
            name=dataset['name'],
            description=dataset['description'],
            created_by=dataset['created_by'],
            created_at=dataset['created_at'],
            updated_at=dataset['updated_at'],
            tags=tags,
            permission_type=permission_type,
            import_status=import_status,
            import_job_id=import_job_id
        )


@router.patch("/{dataset_id}", response_model=UpdateDatasetResponse)
async def update_dataset(
    dataset_id: int = Path(..., description="Dataset ID"),
    request: UpdateDatasetRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    dataset_repo: PostgresDatasetRepository = Depends(get_dataset_repo),
    _: CurrentUser = Depends(require_dataset_write)
) -> UpdateDatasetResponse:
    """Update dataset metadata (name, description, tags)."""
    
    # Check if dataset exists
    dataset = await dataset_repo.get_dataset_by_id(dataset_id)
    if not dataset:
        raise resource_not_found("Dataset", dataset_id)
    
    # Update name and description if provided
    if request.name is not None or request.description is not None:
        await dataset_repo.update_dataset(
            dataset_id=dataset_id,
            name=request.name,
            description=request.description
        )
    
    # Update tags if provided
    if request.tags is not None:
        # Remove all existing tags and add new ones
        await dataset_repo.remove_dataset_tags(dataset_id)
        if request.tags:
            await dataset_repo.add_dataset_tags(dataset_id, request.tags)
    
    # Get updated dataset info
    updated_dataset = await dataset_repo.get_dataset_by_id(dataset_id)
    tags = await dataset_repo.get_dataset_tags(dataset_id)
    
    # Refresh search index to reflect updates
    from ..infrastructure.postgres.search_repository import PostgresSearchRepository
    search_repo = PostgresSearchRepository(dataset_repo._conn)
    await search_repo.refresh_search_index()
    
    return UpdateDatasetResponse(
        dataset_id=dataset_id,
        name=updated_dataset['name'],
        description=updated_dataset['description'],
        metadata={},  # No metadata column in database
        tags=tags,
        updated_at=updated_dataset['updated_at']
    )


@router.delete("/{dataset_id}", response_model=DeleteDatasetResponse)
async def delete_dataset(
    dataset_id: int = Path(..., description="Dataset ID"),
    current_user: CurrentUser = Depends(get_current_user_info),
    dataset_repo: PostgresDatasetRepository = Depends(get_dataset_repo),
    _: CurrentUser = Depends(require_dataset_admin)
) -> DeleteDatasetResponse:
    """Delete a dataset and all its related data."""
    
    # Check if dataset exists
    dataset = await dataset_repo.get_dataset_by_id(dataset_id)
    if not dataset:
        raise resource_not_found("Dataset", dataset_id)
    
    # Delete the dataset
    await dataset_repo.delete_dataset(dataset_id)
    
    # Refresh search index to remove deleted dataset
    from ..infrastructure.postgres.search_repository import PostgresSearchRepository
    search_repo = PostgresSearchRepository(dataset_repo._conn)
    await search_repo.refresh_search_index()
    
    return DeleteDatasetResponse(
        entity_type="Dataset",
        entity_id=dataset_id,
        message=f"Dataset '{dataset['name']}' and all related data have been deleted successfully"
    )


@router.get("/{dataset_id}/ready", response_model=Dict[str, Any])
async def check_dataset_ready(
    dataset_id: int = Path(..., description="Dataset ID"),
    current_user: CurrentUser = Depends(get_current_user_info),
    pool: DatabasePool = Depends(get_db_pool),
    _: CurrentUser = Depends(require_dataset_read)
) -> Dict[str, Any]:
    """Check if a dataset is ready for operations (import completed)."""
    
    from ..infrastructure.postgres import PostgresJobRepository
    async with pool.acquire() as conn:
        job_repo = PostgresJobRepository(conn)
        
        # Check for latest import job
        latest_import_job = await job_repo.get_latest_import_job(dataset_id)
        
        if not latest_import_job:
            # No import job found - dataset might be empty
            return {
                "ready": True,
                "status": "no_import",
                "message": "No import job found for this dataset"
            }
        
        status = latest_import_job['status']
        job_id = str(latest_import_job['job_id'])
        
        if status == 'completed':
            return {
                "ready": True,
                "status": status,
                "import_job_id": job_id,
                "message": "Dataset is ready for use"
            }
        elif status in ['pending', 'processing']:
            return {
                "ready": False,
                "status": status,
                "import_job_id": job_id,
                "message": "Dataset import is still in progress"
            }
        elif status == 'failed':
            return {
                "ready": False,
                "status": status,
                "import_job_id": job_id,
                "message": "Dataset import failed",
                "error": latest_import_job.get('error_message')
            }
        else:
            return {
                "ready": False,
                "status": status,
                "import_job_id": job_id,
                "message": f"Unknown import status: {status}"
            }