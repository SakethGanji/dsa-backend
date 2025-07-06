"""Enhanced dataset management API endpoints with comprehensive validation."""

from fastapi import APIRouter, Depends, HTTPException, status, Path, UploadFile, File, Form
from typing import Annotated, AsyncGenerator
from ..core.database import DatabasePool, UnitOfWorkFactory
from ..core.infrastructure.postgres import PostgresDatasetRepository
from ..features.datasets.grant_permission import GrantPermissionHandler
from ..models.pydantic_models import (
    CreateDatasetResponse, GrantPermissionResponse,
    CurrentUser, ListDatasetsResponse, DatasetSummary,
    DatasetDetailResponse, UpdateDatasetResponse, DeleteDatasetResponse
)
from ..models.validation_models import (
    EnhancedCreateDatasetRequest,
    EnhancedUpdateDatasetRequest,
    EnhancedGrantPermissionRequest,
    PaginationParams,
    ValidatedErrorResponse
)
from ..core.authorization import (
    get_current_user_info,
    require_dataset_admin,
    PermissionType
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/datasets", tags=["datasets"])


# Dependency injection helpers
def get_db_pool() -> DatabasePool:
    """Get database pool."""
    raise NotImplementedError("Database pool not configured")


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


@router.post(
    "/", 
    response_model=CreateDatasetResponse,
    responses={
        400: {"model": ValidatedErrorResponse, "description": "Invalid input"},
        401: {"model": ValidatedErrorResponse, "description": "Not authenticated"},
        422: {"model": ValidatedErrorResponse, "description": "Validation error"}
    }
)
async def create_dataset(
    request: EnhancedCreateDatasetRequest,
    current_user: CurrentUser = Depends(get_current_user_info),
    pool: DatabasePool = Depends(get_db_pool)
) -> CreateDatasetResponse:
    """
    Create a new dataset with optional tags.
    
    - **name**: Dataset name (1-255 characters, alphanumeric with spaces, hyphens, dots, underscores)
    - **description**: Optional description (max 1000 characters)
    - **tags**: Optional list of tags (max 10 tags, each max 50 characters, alphanumeric with hyphens)
    """
    try:
        async with pool.acquire() as conn:
            dataset_repo = PostgresDatasetRepository(conn)
            
            # Check if dataset name already exists for this user
            existing = await dataset_repo.get_dataset_by_name_and_user(
                name=request.name,
                user_id=current_user.user_id
            )
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Dataset with name '{request.name}' already exists"
                )
            
            # Create the dataset
            dataset_id = await dataset_repo.create_dataset(
                name=request.name,
                description=request.description or "",
                created_by=current_user.user_id
            )
            
            # Add tags if provided (deduplicated)
            unique_tags = list(set(request.tags)) if request.tags else []
            if unique_tags:
                await dataset_repo.add_dataset_tags(dataset_id, unique_tags)
            
            # Get the tags back to include in response
            tags = await dataset_repo.get_dataset_tags(dataset_id)
            
            logger.info(f"User {current_user.soeid} created dataset {dataset_id}")
            
            return CreateDatasetResponse(
                dataset_id=dataset_id,
                name=request.name,
                description=request.description,
                tags=tags
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating dataset: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create dataset"
        )


@router.post(
    "/{dataset_id}/permissions",
    response_model=GrantPermissionResponse,
    responses={
        400: {"model": ValidatedErrorResponse, "description": "Invalid input"},
        403: {"model": ValidatedErrorResponse, "description": "Insufficient permissions"},
        404: {"model": ValidatedErrorResponse, "description": "Dataset or user not found"}
    }
)
async def grant_permission(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    request: EnhancedGrantPermissionRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    dataset_repo: PostgresDatasetRepository = Depends(get_dataset_repo)
) -> GrantPermissionResponse:
    """
    Grant permission to a user on a dataset (admin only).
    
    Only dataset admins or system admins can grant permissions.
    """
    # Validate dataset exists
    dataset = await dataset_repo.get_dataset_by_id(dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found"
        )
    
    # Check if current user has admin permission on the dataset
    has_admin = await dataset_repo.check_user_permission(
        dataset_id=dataset_id,
        user_id=current_user.user_id,
        required_permission=PermissionType.ADMIN.value
    )
    
    if not has_admin and not current_user.is_admin():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only dataset admins can grant permissions"
        )
    
    # Validate target user exists
    async with uow_factory.create() as uow:
        target_user = await uow.users.get_by_id(request.user_id)
        if not target_user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with ID {request.user_id} not found"
            )
    
    uow = uow_factory.create()
    handler = GrantPermissionHandler(uow, dataset_repo)
    
    try:
        result = await handler.handle(dataset_id, request, current_user.user_id)
        logger.info(
            f"User {current_user.soeid} granted {request.permission_type} permission "
            f"to user {request.user_id} on dataset {dataset_id}"
        )
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except PermissionError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e)
        )


@router.get(
    "/",
    response_model=ListDatasetsResponse,
    responses={
        401: {"model": ValidatedErrorResponse, "description": "Not authenticated"}
    }
)
async def list_datasets(
    pagination: PaginationParams = Depends(),
    current_user: CurrentUser = Depends(get_current_user_info),
    dataset_repo: PostgresDatasetRepository = Depends(get_dataset_repo)
) -> ListDatasetsResponse:
    """
    List all datasets accessible to the current user with pagination.
    
    Returns datasets where the user has any permission level (read, write, or admin).
    """
    # Get all datasets for the user
    all_datasets = await dataset_repo.list_user_datasets(current_user.user_id)
    
    # Apply pagination
    total = len(all_datasets)
    paginated_datasets = all_datasets[pagination.offset:pagination.offset + pagination.limit]
    
    # Convert to response format with tags
    dataset_summaries = []
    for dataset in paginated_datasets:
        tags = await dataset_repo.get_dataset_tags(dataset['dataset_id'])
        dataset_summaries.append(DatasetSummary(
            dataset_id=dataset['dataset_id'],
            name=dataset['name'],
            description=dataset['description'],
            created_by=dataset['created_by'],
            created_at=dataset['created_at'],
            updated_at=dataset['updated_at'],
            permission_type=dataset['permission_type'],
            tags=tags
        ))
    
    return ListDatasetsResponse(
        datasets=dataset_summaries,
        total=total,
        offset=pagination.offset,
        limit=pagination.limit
    )


@router.get(
    "/{dataset_id}",
    response_model=DatasetDetailResponse,
    responses={
        403: {"model": ValidatedErrorResponse, "description": "No permission to view dataset"},
        404: {"model": ValidatedErrorResponse, "description": "Dataset not found"}
    }
)
async def get_dataset(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    current_user: CurrentUser = Depends(get_current_user_info),
    dataset_repo: PostgresDatasetRepository = Depends(get_dataset_repo)
) -> DatasetDetailResponse:
    """Get detailed information about a specific dataset."""
    # Check if user has read permission
    has_permission = await dataset_repo.check_user_permission(
        dataset_id=dataset_id,
        user_id=current_user.user_id,
        required_permission=PermissionType.READ.value
    )
    
    if not has_permission and not current_user.is_admin():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to view this dataset"
        )
    
    # Get dataset details
    dataset = await dataset_repo.get_dataset_by_id(dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found"
        )
    
    # Get tags
    tags = await dataset_repo.get_dataset_tags(dataset_id)
    
    # Get user's permission type for this dataset
    user_datasets = await dataset_repo.list_user_datasets(current_user.user_id)
    permission_type = None
    for ds in user_datasets:
        if ds['dataset_id'] == dataset_id:
            permission_type = ds['permission_type']
            break
    
    return DatasetDetailResponse(
        dataset_id=dataset['dataset_id'],
        name=dataset['name'],
        description=dataset['description'],
        created_by=dataset['created_by'],
        created_at=dataset['created_at'],
        updated_at=dataset['updated_at'],
        tags=tags,
        permission_type=permission_type
    )


@router.patch(
    "/{dataset_id}",
    response_model=UpdateDatasetResponse,
    responses={
        400: {"model": ValidatedErrorResponse, "description": "Invalid input"},
        403: {"model": ValidatedErrorResponse, "description": "Insufficient permissions"},
        404: {"model": ValidatedErrorResponse, "description": "Dataset not found"}
    }
)
async def update_dataset(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    request: EnhancedUpdateDatasetRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    dataset_repo: PostgresDatasetRepository = Depends(get_dataset_repo)
) -> UpdateDatasetResponse:
    """
    Update dataset metadata (name, description, tags).
    
    Requires write permission on the dataset.
    At least one field must be provided for update.
    """
    # Validate at least one field is being updated
    if not any([request.name, request.description is not None, request.tags is not None]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one field must be provided for update"
        )
    
    # Check if user has write permission
    has_permission = await dataset_repo.check_user_permission(
        dataset_id=dataset_id,
        user_id=current_user.user_id,
        required_permission=PermissionType.WRITE.value
    )
    
    if not has_permission and not current_user.is_admin():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You need write permission to update this dataset"
        )
    
    # Check if dataset exists
    dataset = await dataset_repo.get_dataset_by_id(dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found"
        )
    
    try:
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
            unique_tags = list(set(request.tags)) if request.tags else []
            if unique_tags:
                await dataset_repo.add_dataset_tags(dataset_id, unique_tags)
        
        # Get updated dataset info
        updated_dataset = await dataset_repo.get_dataset_by_id(dataset_id)
        tags = await dataset_repo.get_dataset_tags(dataset_id)
        
        logger.info(f"User {current_user.soeid} updated dataset {dataset_id}")
        
        return UpdateDatasetResponse(
            dataset_id=dataset_id,
            name=updated_dataset['name'],
            description=updated_dataset['description'],
            tags=tags
        )
        
    except Exception as e:
        logger.error(f"Error updating dataset {dataset_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update dataset"
        )


@router.delete(
    "/{dataset_id}",
    response_model=DeleteDatasetResponse,
    responses={
        403: {"model": ValidatedErrorResponse, "description": "Insufficient permissions"},
        404: {"model": ValidatedErrorResponse, "description": "Dataset not found"}
    }
)
async def delete_dataset(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    current_user: CurrentUser = Depends(get_current_user_info),
    dataset_repo: PostgresDatasetRepository = Depends(get_dataset_repo)
) -> DeleteDatasetResponse:
    """
    Delete a dataset and all its related data.
    
    Requires admin permission on the dataset.
    This operation is irreversible and will cascade delete all related data.
    """
    # Check if user has admin permission on dataset or is system admin
    has_permission = await dataset_repo.check_user_permission(
        dataset_id=dataset_id,
        user_id=current_user.user_id,
        required_permission=PermissionType.ADMIN.value
    )
    
    if not has_permission and not current_user.is_admin():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only dataset admins can delete datasets"
        )
    
    # Check if dataset exists
    dataset = await dataset_repo.get_dataset_by_id(dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found"
        )
    
    try:
        # Delete the dataset
        await dataset_repo.delete_dataset(dataset_id)
        
        logger.info(f"User {current_user.soeid} deleted dataset {dataset_id} ({dataset['name']})")
        
        return DeleteDatasetResponse(
            dataset_id=dataset_id,
            message=f"Dataset '{dataset['name']}' and all related data have been deleted successfully"
        )
        
    except Exception as e:
        logger.error(f"Error deleting dataset {dataset_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete dataset"
        )