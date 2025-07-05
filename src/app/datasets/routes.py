"""Routes for datasets API v2 - Git-like versioning system"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query, Path, Body, status
from fastapi.responses import StreamingResponse
from typing import List, Optional, Dict, Any, Annotated
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
import uuid

from app.datasets.controller import DatasetsController
from app.datasets.service import DatasetsService
from app.datasets.repository import DatasetsRepository
from app.datasets.models import (
    Dataset, DatasetUpdate,
    Tag, DatasetStatistics
)
from app.users.models import DatasetPermission, PermissionGrant, DatasetPermissionType
from app.datasets.constants import DEFAULT_PAGE_SIZE, MAX_ROWS_PER_PAGE
from app.db.connection import get_session
from app.storage.factory import StorageFactory
from app.users.auth import get_current_user_info, CurrentUser
from app.datasets.search.routes import router as search_router

import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/datasets", tags=["Datasets"])

# Include search sub-router
router.include_router(search_router)

# Dependency injection - simplified like sampling slice
async def get_controller(session: AsyncSession = Depends(get_session)) -> DatasetsController:
    """Get datasets controller instance"""
    from app.users.service import UserService
    
    repository = DatasetsRepository(session)
    storage_backend = StorageFactory.get_instance()
    user_service = UserService(session)
    service = DatasetsService(repository, storage_backend, user_service)
    return DatasetsController(service)

# Type aliases for cleaner code
ControllerDep = Annotated[DatasetsController, Depends(get_controller)]
UserDep = Annotated[CurrentUser, Depends(get_current_user_info)]


@router.post(
    "/{dataset_id}/refs/{ref_name}/uploads",
    response_model=Dict[str, Any],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Upload data to a dataset branch",
    description="""Upload a file to create a new commit on a branch. 
    This is an asynchronous operation that returns a job ID."""
)
async def upload_to_branch(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: str = Path(..., description="Branch/ref name (e.g., 'main')"),
    file: UploadFile = File(..., description="The dataset file to upload"),
    message: str = Form(..., description="Commit message"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Upload a file to create a new commit on a branch."""
    # This will be implemented to:
    # 1. Create an import job in analysis_runs table
    # 2. Return job_id for status tracking
    # 3. Background worker processes file and creates commit
    raise NotImplementedError("Upload to branch endpoint - returns job_id")


@router.get(
    "",
    response_model=List[Dataset],
    summary="List datasets",
    description="Retrieve a paginated list of datasets with optional filtering"
)
async def list_datasets(
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=100, description="Page size"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    name: Optional[str] = Query(None, description="Filter by name (partial match)"),
    description: Optional[str] = Query(None, description="Filter by description (partial match)"),
    created_by: Optional[int] = Query(None, description="Filter by creator user ID"),
    tags: Optional[List[str]] = Query(None, description="Filter by tags"),
    sort_by: Optional[str] = Query(None, description="Sort field: name, created_at, updated_at"),
    sort_order: Optional[str] = Query(None, description="Sort order: asc or desc"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[Dataset]:
    """Retrieve datasets with filtering, sorting, and pagination."""
    return await controller.list_datasets(
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order,
        name=name,
        description=description,
        created_by=created_by,
        tags=tags,
        current_user=current_user
    )


@router.get(
    "/{dataset_id}",
    response_model=Dataset,
    summary="Get dataset details",
    description="Retrieve detailed information about a specific dataset"
)
async def get_dataset(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dataset:
    """Get complete dataset information including refs and metadata."""
    return await controller.get_dataset(dataset_id, current_user)


@router.patch(
    "/{dataset_id}",
    response_model=Dataset,
    summary="Update dataset",
    description="Update dataset metadata"
)
async def update_dataset(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    data: DatasetUpdate = Body(..., description="Updated dataset information"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dataset:
    """Update dataset name, description, and tags."""
    return await controller.update_dataset(dataset_id, data, current_user)


@router.delete(
    "/{dataset_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete dataset",
    description="Delete an entire dataset and all its commits (requires admin permission)"
)
async def delete_dataset(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> None:
    """Delete a dataset and all associated data. This operation cannot be undone."""
    await controller.delete_dataset(dataset_id, current_user)


@router.get(
    "/{dataset_id}/commits",
    response_model=List[Dict[str, Any]],
    summary="List dataset commits",
    description="Get commit history for a dataset"
)
async def list_dataset_commits(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    ref_name: Optional[str] = Query(None, description="Filter by ref (branch/tag)"),
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=100),
    offset: int = Query(0, ge=0),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[Dict[str, Any]]:
    """Retrieve commit history for a dataset."""
    # Returns list of commits with commit_id, message, author, timestamp
    raise NotImplementedError("List commits endpoint")


@router.get(
    "/{dataset_id}/refs",
    response_model=List[Dict[str, Any]],
    summary="List dataset refs",
    description="List all refs (branches and tags) for a dataset"
)
async def list_dataset_refs(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[Dict[str, Any]]:
    """List all refs (branches and tags) for a dataset."""
    # Returns list of refs with name, commit_id, type (branch/tag)
    raise NotImplementedError("List refs endpoint")


@router.get(
    "/{dataset_id}/refs/{ref_name}",
    response_model=Dict[str, Any],
    summary="Get ref details",
    description="Get details about a specific ref"
)
async def get_ref_details(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    ref_name: str = Path(..., description="Ref name (e.g., 'main', 'v1.0')"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Get details about a specific ref."""
    # Returns ref details including commit it points to
    raise NotImplementedError("Get ref details endpoint")


@router.get(
    "/{dataset_id}/commits/{commit_hash}",
    response_model=Dict[str, Any],
    summary="Get commit details",
    description="Get detailed information about a specific commit"
)
async def get_commit_details(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    commit_hash: str = Path(..., regex="^[a-f0-9]{64}$", description="Commit SHA256 hash"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Get commit details including parent, message, and manifest."""
    raise NotImplementedError("Get commit details endpoint")


@router.get(
    "/{dataset_id}/refs/{ref_name}/download",
    summary="Download dataset from ref",
    description="Download the dataset file from a specific ref (branch/tag)"
)
async def download_dataset_from_ref(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    ref_name: str = Path(..., description="Ref name (e.g., 'main', 'v1.0')"),
    controller: ControllerDep = None,
    current_user: UserDep = None
):
    """Stream the dataset file for download from a ref."""
    # This will:
    # 1. Resolve ref to commit
    # 2. Get commit manifest
    # 3. Reconstruct file from rows
    # 4. Stream as download
    raise NotImplementedError("Download from ref endpoint")


@router.delete(
    "/{dataset_id}/refs/{ref_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a ref",
    description="Delete a branch or tag (commits remain in history)"
)
async def delete_ref(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    ref_name: str = Path(..., description="Ref name to delete"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> None:
    """Delete a ref. Commits remain accessible by hash."""
    raise NotImplementedError("Delete ref endpoint")


@router.get(
    "/{dataset_id}/refs/{ref_name}/data",
    summary="Get data from ref",
    description="Retrieve paginated data from a dataset ref"
)
async def get_data_from_ref(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    ref_name: str = Path(..., description="Ref name (e.g., 'main')"),
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_ROWS_PER_PAGE, description="Rows per page"),
    offset: int = Query(0, ge=0, description="Number of rows to skip"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Get paginated data from a ref."""
    # This will:
    # 1. Resolve ref to commit
    # 2. Get commit manifest
    # 3. Fetch rows and reconstruct data
    raise NotImplementedError("Get data from ref endpoint")


@router.get(
    "/{dataset_id}/commits/{commit_hash}/schema",
    response_model=Dict[str, Any],
    summary="Get commit schema",
    description="Get schema information for a specific commit"
)
async def get_commit_schema(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    commit_hash: str = Path(..., regex="^[a-f0-9]{64}$", description="Commit SHA256 hash"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Get schema information for a commit."""
    raise NotImplementedError("Get commit schema endpoint")


@router.get(
    "/{dataset_id}/commits/diff",
    response_model=Dict[str, Any],
    summary="Diff between commits",
    description="Get differences between two commits"
)
async def diff_commits(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    from_commit: str = Query(..., regex="^[a-f0-9]{64}$", description="From commit hash"),
    to_commit: str = Query(..., regex="^[a-f0-9]{64}$", description="To commit hash"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Get diff between two commits showing added/removed/modified rows."""
    raise NotImplementedError("Diff commits endpoint")


# Permission operations
@router.get(
    "/{dataset_id}/permissions",
    response_model=List[DatasetPermission],
    summary="List dataset permissions",
    description="List all permissions for a dataset"
)
async def list_dataset_permissions(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[DatasetPermission]:
    """Get all permissions granted for this dataset."""
    # Check if user has admin permission to view permissions
    user_id = await controller.service.get_user_id_from_soeid(current_user.soeid)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not found"
        )
    
    has_permission = await controller.service.check_dataset_permission(dataset_id, user_id, "admin")
    if not has_permission:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin permission required to view permissions"
        )
    
    return await controller.service.user_service.list_dataset_permissions(dataset_id)


@router.post(
    "/{dataset_id}/permissions",
    response_model=DatasetPermission,
    summary="Grant permission",
    description="Grant permission on a dataset to a user"
)
async def grant_dataset_permission(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    grant: PermissionGrant = Body(...),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> DatasetPermission:
    """Grant permission to a user for this dataset (requires admin permission)."""
    # Check if current user has admin permission
    user_id = await controller.service.get_user_id_from_soeid(current_user.soeid)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not found"
        )
    
    has_permission = await controller.service.check_dataset_permission(dataset_id, user_id, "admin")
    if not has_permission:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin permission required to grant permissions"
        )
    
    # Verify dataset exists
    dataset = await controller.get_dataset(dataset_id)
    
    from app.users.models import DatasetPermissionType
    perm_type = DatasetPermissionType(grant.permission_type)
    return await controller.service.user_service.grant_dataset_permission(
        dataset_id,
        grant.user_id,
        perm_type
    )


@router.delete(
    "/{dataset_id}/permissions/{user_id}",
    response_model=Dict[str, Any],
    summary="Revoke all permissions",
    description="Revoke all permissions from a user for this dataset"
)
async def revoke_all_permissions(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    user_id: int = Path(..., description="User ID to revoke permissions from"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Revoke all permissions from a user for this dataset."""
    # Check if current user has admin permission
    current_user_id = await controller.service.get_user_id_from_soeid(current_user.soeid)
    if not current_user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not found"
        )
    
    has_permission = await controller.service.check_dataset_permission(dataset_id, current_user_id, "admin")
    if not has_permission:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin permission required to revoke permissions"
        )
    
    # Don't allow revoking own permissions
    if user_id == current_user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot revoke your own permissions"
        )
    
    # Implementation would revoke all permissions for user
    raise NotImplementedError("Revoke all permissions endpoint")


# Ref Management Routes
@router.post(
    "/{dataset_id}/refs",
    response_model=Dict[str, Any],
    summary="Create a new ref",
    description="Create a new branch or tag pointing to a commit"
)
async def create_ref(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    ref_name: str = Body(..., description="Ref name (e.g., 'feature-branch', 'v1.0')"),
    commit_hash: str = Body(..., regex="^[a-f0-9]{64}$", description="Commit hash to point to"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Create a new ref pointing to a commit."""
    raise NotImplementedError("Create ref endpoint")


@router.put(
    "/{dataset_id}/refs/{ref_name}",
    response_model=Dict[str, Any],
    summary="Update ref",
    description="Update a ref to point to a different commit"
)
async def update_ref(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    ref_name: str = Path(..., description="Ref name to update"),
    commit_hash: str = Body(..., regex="^[a-f0-9]{64}$", description="New commit hash"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Update a ref to point to a different commit."""
    raise NotImplementedError("Update ref endpoint")


# Statistics Routes (Now async job-based)
@router.post(
    "/{dataset_id}/commits/{commit_hash}/explorations",
    response_model=Dict[str, Any],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create exploration job",
    description="Create an async job to explore/profile a dataset commit"
)
async def create_exploration_job(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    commit_hash: str = Path(..., regex="^[a-f0-9]{64}$", description="Commit hash"),
    parameters: Dict[str, Any] = Body(default={}, description="Exploration parameters"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Create an async exploration job for a commit."""
    # Returns job_id for status tracking via /jobs endpoint
    raise NotImplementedError("Create exploration job endpoint")


# Job Management Routes (moved here for dataset-specific jobs)
@router.get(
    "/{dataset_id}/jobs",
    response_model=List[Dict[str, Any]],
    summary="List dataset jobs",
    description="List all jobs for a specific dataset"
)
async def list_dataset_jobs(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    job_type: Optional[str] = Query(None, enum=["import", "sampling", "exploration", "profiling"]),
    status: Optional[str] = Query(None, enum=["pending", "running", "completed", "failed"]),
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=100),
    offset: int = Query(0, ge=0),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[Dict[str, Any]]:
    """List all jobs for a dataset with optional filtering."""
    raise NotImplementedError("List dataset jobs endpoint")