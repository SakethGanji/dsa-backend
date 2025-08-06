"""Dataset management API endpoints."""

from fastapi import APIRouter, Depends, status, Path, UploadFile, File, Form
from typing import Annotated, AsyncGenerator, Dict, Any
from ..infrastructure.postgres.database import DatabasePool, UnitOfWorkFactory
from ..infrastructure.postgres import PostgresDatasetRepository
from ..features.datasets.services import DatasetService
from ..api.models import (
    CreateDatasetRequest, CreateDatasetResponse,
    GrantPermissionRequest, GrantPermissionResponse,
    CurrentUser, ListDatasetsResponse, DatasetSummary,
    DatasetDetailResponse, UpdateDatasetRequest,
    UpdateDatasetResponse, DeleteDatasetResponse,
    CreateDatasetWithFileResponse
)
from ..core.authorization import (
    get_current_user_info,
    require_dataset_admin,
    require_dataset_read,
    require_dataset_write,
    PermissionType
)
from ..core.domain_exceptions import resource_not_found, ConflictException
from .dependencies import get_db_pool, get_event_bus, get_permission_service


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
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    event_bus = Depends(get_event_bus),
    permission_service = Depends(get_permission_service)
) -> CreateDatasetResponse:
    """Create a new dataset with optional tags."""
    from ..features.datasets.models import CreateDatasetCommand
    
    # Create command from request
    command = CreateDatasetCommand(
        created_by=current_user.user_id,
        name=request.name,
        description=request.description or "",
        tags=request.tags
    )
    
    # Create unit of work and service
    async with uow_factory.create() as uow:
        service = DatasetService(
            uow=uow,
            permissions=permission_service,
            event_bus=event_bus
        )
        
        # Execute service method
        return await service.create_dataset(command)


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
    event_bus = Depends(get_event_bus),
    permission_service = Depends(get_permission_service)
) -> CreateDatasetWithFileResponse:
    """Create a new dataset and import a file in one operation."""
    from ..features.datasets.models import CreateDatasetWithFileCommand
    
    # Parse tags from comma-separated string
    tag_list = [tag.strip() for tag in tags.split(",")] if tags else []
    
    # Create request object - using command directly since request model is missing
    # request = CreateDatasetWithFileRequest(
    #     name=name,
    #     description=description,
    #     tags=tag_list,
    #     default_branch=default_branch,
    #     commit_message=commit_message
    # )
    
    # Create command
    command = CreateDatasetWithFileCommand(
        name=name,
        created_by=current_user.user_id,
        file_name=file.filename,
        file_size=file.size or 0,
        file_content=file.file,
        description=description,
        tags=tag_list,
        default_branch=default_branch,
        branch_name="main",
        commit_message=commit_message
    )
    
    # Create unit of work and service
    async with uow_factory.create() as uow:
        service = DatasetService(
            uow=uow,
            permissions=permission_service,
            event_bus=event_bus
        )
        
        # Execute service method
        return await service.create_dataset_with_file(command)


@router.post("/{dataset_id}/permissions", response_model=GrantPermissionResponse)
async def grant_permission(
    dataset_id: int = Path(..., description="Dataset ID"),
    request: GrantPermissionRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    event_bus = Depends(get_event_bus),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_admin)
) -> GrantPermissionResponse:
    """Grant permission to a user on a dataset (admin only)."""
    from ..features.datasets.models import GrantPermissionCommand
    
    # Create command
    command = GrantPermissionCommand(
        dataset_id=dataset_id,
        user_id=current_user.user_id,
        target_user_id=request.user_id,
        permission_type=request.permission_type
    )
    
    # Create unit of work and service
    async with uow_factory.create() as uow:
        service = DatasetService(
            uow=uow,
            permissions=permission_service,
            event_bus=event_bus
        )
        
        # Execute service method
        return await service.grant_permission(command)


@router.get("/", response_model=ListDatasetsResponse)
async def list_datasets(
    offset: int = 0,
    limit: int = 100,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    pool: DatabasePool = Depends(get_db_pool),
    event_bus = Depends(get_event_bus),
    permission_service = Depends(get_permission_service)
) -> ListDatasetsResponse:
    """List all datasets accessible to the current user with pagination."""
    from ..features.datasets.models import ListDatasetsCommand
    
    # Create command
    command = ListDatasetsCommand(
        user_id=current_user.user_id,
        offset=offset,
        limit=limit
    )
    
    # Create unit of work and service
    async with uow_factory.create() as uow:
        service = DatasetService(
            uow=uow,
            permissions=permission_service,
            event_bus=event_bus
        )
        
        # Execute service method
        dataset_items, total = await service.list_datasets(command)
        
        # Get import status for each dataset
        # Note: This is done outside the handler to maintain separation of concerns
        from ..infrastructure.postgres import PostgresJobRepository
        dataset_summaries = []
        
        async with pool.acquire() as conn:
            job_repo = PostgresJobRepository(conn)
            
            for item in dataset_items:
                # Check for latest import job
                import_status = None
                import_job_id = None
                latest_import_job = await job_repo.get_latest_import_job(item.dataset_id)
                if latest_import_job:
                    import_job_id = str(latest_import_job['job_id'])
                    import_status = latest_import_job['status']
                
                dataset_summaries.append(DatasetSummary(
                    dataset_id=item.dataset_id,
                    name=item.name,
                    description=item.description,
                    created_by=item.created_by,
                    created_at=item.created_at,
                    updated_at=item.updated_at,
                    permission_type=item.permission_type,
                    tags=item.tags,
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
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    pool: DatabasePool = Depends(get_db_pool),
    event_bus = Depends(get_event_bus),
    permission_service = Depends(get_permission_service)
) -> DatasetDetailResponse:
    """Get detailed information about a specific dataset."""
    from ..features.datasets.models import GetDatasetCommand
    
    # Create command
    command = GetDatasetCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id
    )
    
    # Create unit of work and service
    async with uow_factory.create() as uow:
        service = DatasetService(
            uow=uow,
            permissions=permission_service,
            event_bus=event_bus
        )
        
        # Execute service method
        response = await service.get_dataset(command)
    
    # Get import job status (done outside handler to maintain separation)
    async with pool.acquire() as conn:
        from ..infrastructure.postgres import PostgresJobRepository
        job_repo = PostgresJobRepository(conn)
        latest_import_job = await job_repo.get_latest_import_job(dataset_id)
        
        if latest_import_job:
            response.import_job_id = str(latest_import_job['job_id'])
            response.import_status = latest_import_job['status']
    
    return response


@router.patch("/{dataset_id}", response_model=UpdateDatasetResponse)
async def update_dataset(
    dataset_id: int = Path(..., description="Dataset ID"),
    request: UpdateDatasetRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    event_bus = Depends(get_event_bus),
    permission_service = Depends(get_permission_service)
) -> UpdateDatasetResponse:
    """Update dataset metadata (name, description, tags)."""
    from ..features.datasets.models import UpdateDatasetCommand
    
    # Create command from request
    command = UpdateDatasetCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id,
        name=request.name,
        description=request.description,
        tags=request.tags
    )
    
    # Create unit of work and service
    async with uow_factory.create() as uow:
        service = DatasetService(
            uow=uow,
            permissions=permission_service,
            event_bus=event_bus
        )
        
        # Execute service method
        return await service.update_dataset(command)


@router.delete("/{dataset_id}", response_model=DeleteDatasetResponse)
async def delete_dataset(
    dataset_id: int = Path(..., description="Dataset ID"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    event_bus = Depends(get_event_bus),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_admin)
) -> DeleteDatasetResponse:
    """Delete a dataset and all its related data."""
    from ..features.datasets.models import DeleteDatasetCommand
    from ..features.datasets.services.dataset_service import DeleteDatasetResponse as ServiceDeleteResponse
    
    # Create command
    command = DeleteDatasetCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id
    )
    
    # Create unit of work and service
    async with uow_factory.create() as uow:
        service = DatasetService(
            uow=uow,
            permissions=permission_service,
            event_bus=event_bus
        )
        
        # Execute service method - returns the service's DeleteDatasetResponse
        service_response = await service.delete_dataset(command)
        
        # Convert to API's DeleteDatasetResponse
        return DeleteDatasetResponse(
            success=True,
            message=service_response.message
        )


@router.get("/{dataset_id}/ready", response_model=Dict[str, Any])
async def check_dataset_ready(
    dataset_id: int = Path(..., description="Dataset ID"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    event_bus = Depends(get_event_bus),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_read)
) -> Dict[str, Any]:
    """Check if a dataset is ready for operations (import completed)."""
    from ..features.datasets.models import CheckDatasetReadyCommand
    
    # Create command
    command = CheckDatasetReadyCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id
    )
    
    # Create unit of work and service
    async with uow_factory.create() as uow:
        service = DatasetService(
            uow=uow,
            permissions=permission_service,
            event_bus=event_bus
        )
        
        # Execute service method
        return await service.check_dataset_ready(command)