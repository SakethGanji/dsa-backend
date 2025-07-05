"""Dataset management API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status, Path, UploadFile, File, Form
from typing import Annotated, AsyncGenerator
from ..core.database import DatabasePool, UnitOfWorkFactory
from ..core.services.postgres import PostgresDatasetRepository
from ..features.datasets.grant_permission import GrantPermissionHandler
from ..models.pydantic_models import (
    CreateDatasetRequest, CreateDatasetResponse,
    GrantPermissionRequest, GrantPermissionResponse,
    CurrentUser
)
from ..core.authorization import (
    get_current_user_info,
    require_dataset_admin,
    PermissionType
)


router = APIRouter(prefix="/datasets", tags=["datasets"])


# Dependency injection helpers (will be overridden in main.py)
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


@router.post("/", response_model=CreateDatasetResponse)
async def create_dataset(
    request: CreateDatasetRequest,
    current_user: CurrentUser = Depends(get_current_user_info),
    pool: DatabasePool = Depends(get_db_pool)
) -> CreateDatasetResponse:
    """Create a new dataset."""
    async with pool.acquire() as conn:
        dataset_repo = PostgresDatasetRepository(conn)
        
        dataset_id = await dataset_repo.create_dataset(
            name=request.name,
            description=request.description or "",
            created_by=current_user.user_id
        )
        
        return CreateDatasetResponse(
            dataset_id=dataset_id,
            name=request.name
        )


@router.post("/{dataset_id}/permissions", response_model=GrantPermissionResponse)
async def grant_permission(
    dataset_id: int = Path(..., description="Dataset ID"),
    request: GrantPermissionRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    dataset_repo: PostgresDatasetRepository = Depends(get_dataset_repo)
) -> GrantPermissionResponse:
    """Grant permission to a user on a dataset (admin only)."""
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
    
    uow = uow_factory.create()
    handler = GrantPermissionHandler(uow, dataset_repo)
    
    try:
        # Handler expects granting_user_id parameter
        return await handler.handle(dataset_id, request, current_user.user_id)
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