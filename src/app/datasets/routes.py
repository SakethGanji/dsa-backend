"""Routes for datasets API - simplified following sampling slice pattern"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query, Path, Body, status
from fastapi.responses import StreamingResponse
from typing import List, Optional, Dict, Any, Annotated
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession

from app.datasets.controller import DatasetsController
from app.datasets.service import DatasetsService
from app.datasets.repository import DatasetsRepository
from app.datasets.models import (
    Dataset, DatasetUploadResponse, DatasetUpdate,
    DatasetVersion, Tag, SheetInfo, SchemaVersion, VersionFile, DatasetPointer
)
from app.users.models import DatasetPermission, PermissionGrant, DatasetPermissionType
from app.datasets.constants import DEFAULT_PAGE_SIZE, MAX_ROWS_PER_PAGE
from app.db.connection import get_session
from app.storage.factory import StorageFactory
from app.users.auth import get_current_user_info, CurrentUser

import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/datasets", tags=["Datasets"])

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
    "/upload",
    response_model=DatasetUploadResponse,
    summary="Upload a dataset",
    description="Upload a new dataset or create a new version of an existing dataset"
)
async def upload_dataset(
    file: UploadFile = File(..., description="The dataset file to upload"),
    dataset_id: Optional[int] = Form(None, description="ID of existing dataset for versioning"),
    name: str = Form(..., description="Name of the dataset"),
    description: Optional[str] = Form(None, description="Description of the dataset"),
    tags: Optional[str] = Form(None, description="Tags as JSON array or comma-separated string"),
    parent_version_id: Optional[int] = Form(None, description="Parent version ID for branching"),
    message: Optional[str] = Form(None, description="Version message/description"),
    branch_name: Optional[str] = Form("main", description="Target branch for the upload"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> DatasetUploadResponse:
    """Upload a new dataset or create a new version of an existing dataset."""
    return await controller.upload_dataset(
        file=file,
        current_user=current_user,
        dataset_id=dataset_id,
        name=name,
        description=description,
        tags=tags,
        parent_version_id=parent_version_id,
        message=message,
        branch_name=branch_name
    )


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
    sort_by: Optional[str] = Query(None, description="Sort field: name, created_at, updated_at, file_size, current_version"),
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
    "/tags",
    response_model=List[Tag],
    summary="List all tags",
    description="Retrieve all available tags with usage counts"
)
async def list_tags(
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[Tag]:
    """Get all tags used across datasets."""
    return await controller.list_tags()


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
    """Get complete dataset information including versions and tags."""
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
    description="Delete an entire dataset and all its versions (requires admin permission)"
)
async def delete_dataset(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> None:
    """Delete a dataset and all associated data. This operation cannot be undone."""
    await controller.delete_dataset(dataset_id, current_user)


@router.get(
    "/{dataset_id}/versions",
    response_model=List[DatasetVersion],
    summary="List dataset versions",
    description="Get all versions of a dataset"
)
async def list_dataset_versions(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[DatasetVersion]:
    """Retrieve all versions for a specific dataset."""
    return await controller.list_dataset_versions(dataset_id)


@router.get(
    "/{dataset_id}/versions/tree",
    response_model=Dict[str, Any],
    summary="Get version tree",
    description="Get version tree/DAG structure showing parent-child relationships"
)
async def get_version_tree(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Get version tree structure showing branching and parent relationships."""
    return await controller.get_version_tree(dataset_id)


@router.get(
    "/{dataset_id}/versions/{version_id}",
    response_model=DatasetVersion,
    summary="Get dataset version",
    description="Get detailed information about a specific version"
)
async def get_dataset_version(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    version_id: int = Path(..., gt=0, description="Version ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> DatasetVersion:
    """Get version details including sheets and metadata."""
    return await controller.get_version_for_dataset(dataset_id, version_id)


@router.get(
    "/{dataset_id}/versions/{version_id}/download",
    summary="Download dataset version",
    description="Download the raw file for a specific version"
)
async def download_dataset_version(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    version_id: int = Path(..., gt=0, description="Version ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
):
    """Stream the dataset file for download."""
    version = await controller.get_version_for_dataset(dataset_id, version_id)
    file_info = await controller.get_dataset_version_file(version_id)
    if not file_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File data not found"
        )
    
    # Handle filesystem storage
    if file_info.storage_type == "filesystem" and file_info.file_path:
        import asyncio
        
        # Update file extension to .parquet since we store all files as Parquet
        file_name = f"{version.dataset_id}_v{version.version_number}.parquet"
        media_type = "application/parquet"
        
        async def iter_file():
            loop = asyncio.get_event_loop()
            with open(file_info.file_path, 'rb') as f:
                chunk_size = 1024 * 1024  # 1MB chunks
                while True:
                    chunk = await loop.run_in_executor(None, f.read, chunk_size)
                    if not chunk:
                        break
                    yield chunk
        
        return StreamingResponse(
            iter_file(),
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={file_name}",
                "Content-Length": str(file_info.file_size) if file_info.file_size else None
            }
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File data not found or unsupported storage type"
        )


@router.delete(
    "/{dataset_id}/versions/{version_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete dataset version",
    description="Delete a specific version of a dataset"
)
async def delete_dataset_version(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    version_id: int = Path(..., gt=0, description="Version ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> None:
    """Delete a dataset version. This operation cannot be undone."""
    await controller.get_version_for_dataset(dataset_id, version_id)
    await controller.delete_dataset_version(version_id, current_user)


@router.get(
    "/{dataset_id}/versions/{version_id}/sheets",
    response_model=List[SheetInfo],
    summary="List version sheets",
    description="Get all sheets in a dataset version"
)
async def list_version_sheets(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    version_id: int = Path(..., gt=0, description="Version ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[SheetInfo]:
    """List all sheets/tables in the dataset version."""
    await controller.get_version_for_dataset(dataset_id, version_id)
    return await controller.list_version_sheets(version_id)


@router.get(
    "/{dataset_id}/versions/{version_id}/data",
    summary="Get sheet data",
    description="Retrieve paginated data from a dataset sheet"
)
async def get_sheet_data(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    version_id: int = Path(..., gt=0, description="Version ID"),
    sheet: Optional[str] = Query(None, description="Sheet name (optional for single-sheet files)"),
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_ROWS_PER_PAGE, description="Rows per page"),
    offset: int = Query(0, ge=0, description="Number of rows to skip"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Get paginated data from a specific sheet in the dataset."""
    await controller.get_version_for_dataset(dataset_id, version_id)
    return await controller.get_sheet_data(
        version_id=version_id,
        sheet_name=sheet,
        limit=limit,
        offset=offset
    )


@router.get(
    "/{dataset_id}/versions/{version_id}/schema",
    response_model=SchemaVersion,
    summary="Get version schema",
    description="Get schema information for a dataset version"
)
async def get_version_schema(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    version_id: int = Path(..., gt=0, description="Version ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> SchemaVersion:
    """Get schema information including column names, types, and metadata."""
    await controller.get_version_for_dataset(dataset_id, version_id)
    return await controller.get_schema_for_version(version_id)


@router.post(
    "/{dataset_id}/schema/compare",
    response_model=Dict[str, Any],
    summary="Compare version schemas",
    description="Compare schemas between two dataset versions"
)
async def compare_schemas(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    comparison_request: Dict[str, int] = Body(..., example={"version1_id": 1, "version2_id": 2}),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Compare schemas to identify added/removed columns and type changes."""
    version1_id = comparison_request.get("version1_id")
    version2_id = comparison_request.get("version2_id")
    
    if not version1_id or not version2_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Both version1_id and version2_id are required"
        )
    
    # Verify both versions belong to the dataset
    await controller.get_version_for_dataset(dataset_id, version1_id)
    await controller.get_version_for_dataset(dataset_id, version2_id)
    return await controller.compare_version_schemas(version1_id, version2_id)


@router.post(
    "/{dataset_id}/versions/{version_id}/files",
    response_model=Dict[str, Any],
    summary="Attach file to version",
    description="Attach an additional file to an existing dataset version"
)
async def attach_file_to_version(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    version_id: int = Path(..., gt=0, description="Version ID"),
    file: UploadFile = File(..., description="File to attach"),
    component_type: str = Form(..., description="Component type (e.g., metadata, schema, supplement)"),
    component_name: Optional[str] = Form(None, description="Optional component name"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Attach additional files to a dataset version for multi-file support."""
    await controller.get_version_for_dataset(dataset_id, version_id)
    return await controller.attach_file_to_version(
        version_id=version_id,
        file=file,
        component_type=component_type,
        component_name=component_name,
        current_user=current_user
    )


@router.get(
    "/{dataset_id}/versions/{version_id}/files",
    response_model=List[VersionFile],
    summary="List version files",
    description="List all files attached to a dataset version"
)
async def list_version_files(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    version_id: int = Path(..., gt=0, description="Version ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[VersionFile]:
    """Get all files attached to a specific version."""
    await controller.get_version_for_dataset(dataset_id, version_id)
    return await controller.list_version_files(version_id)


@router.get(
    "/{dataset_id}/versions/{version_id}/files/{component_type}",
    response_model=VersionFile,
    summary="Get specific file",
    description="Get a specific file by component type"
)
async def get_version_file(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    version_id: int = Path(..., gt=0, description="Version ID"),
    component_type: str = Path(..., description="Component type"),
    component_name: Optional[str] = Query(None, description="Optional component name"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> VersionFile:
    """Get a specific file from a version by component type and optional name."""
    await controller.get_version_for_dataset(dataset_id, version_id)
    return await controller.get_version_file(version_id, component_type, component_name)


# Branch and Tag operations
@router.post(
    "/{dataset_id}/branches",
    response_model=DatasetPointer,
    summary="Create branch",
    description="Create a new branch pointing to a specific version"
)
async def create_branch(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    request: Dict[str, Any] = Body(..., example={"branch_name": "feature-branch", "from_version_id": 1}),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> DatasetPointer:
    """Create a new branch from an existing version."""
    branch_name = request.get("branch_name")
    from_version_id = request.get("from_version_id")
    
    if not branch_name or not from_version_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Both branch_name and from_version_id are required"
        )
    
    return await controller.create_branch(dataset_id, branch_name, from_version_id, current_user)


@router.post(
    "/{dataset_id}/tags",
    response_model=DatasetPointer,
    summary="Create tag",
    description="Create an immutable tag pointing to a specific version"
)
async def create_tag(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    request: Dict[str, Any] = Body(..., example={"tag_name": "v1.0", "version_id": 1}),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> DatasetPointer:
    """Create an immutable tag for a specific version."""
    tag_name = request.get("tag_name")
    version_id = request.get("version_id")
    
    if not tag_name or not version_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Both tag_name and version_id are required"
        )
    
    return await controller.create_tag(dataset_id, tag_name, version_id, current_user)


@router.patch(
    "/{dataset_id}/branches/{branch_name:path}",
    response_model=Dict[str, Any],
    summary="Update branch",
    description="Update a branch to point to a different version"
)
async def update_branch(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    branch_name: str = Path(..., description="Branch name to update"),
    request: Dict[str, int] = Body(..., example={"to_version_id": 2}),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Move a branch pointer to a different version."""
    to_version_id = request.get("to_version_id")
    
    if not to_version_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="to_version_id is required"
        )
    
    return await controller.update_branch(dataset_id, branch_name, to_version_id, current_user)


@router.get(
    "/{dataset_id}/pointers",
    response_model=List[DatasetPointer],
    summary="List pointers",
    description="List all branches and tags for a dataset"
)
async def list_dataset_pointers(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[DatasetPointer]:
    """Get all branches and tags for a dataset."""
    return await controller.list_dataset_pointers(dataset_id)


@router.get(
    "/{dataset_id}/pointers/{pointer_name}",
    response_model=DatasetPointer,
    summary="Get pointer",
    description="Get details of a specific branch or tag"
)
async def get_pointer(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    pointer_name: str = Path(..., description="Pointer name (branch or tag)"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> DatasetPointer:
    """Get information about a specific branch or tag."""
    return await controller.get_pointer(dataset_id, pointer_name)


@router.delete(
    "/{dataset_id}/pointers/{pointer_name}",
    response_model=Dict[str, Any],
    summary="Delete pointer",
    description="Delete a branch or tag"
)
async def delete_pointer(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    pointer_name: str = Path(..., description="Pointer name to delete"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Delete a branch or tag (cannot delete 'main' branch)."""
    return await controller.delete_pointer(dataset_id, pointer_name, current_user)


@router.get(
    "/{dataset_id}/branches",
    response_model=List[DatasetPointer],
    summary="List branches",
    description="List all branches for a dataset"
)
async def list_branches(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[DatasetPointer]:
    """Get all branches (not tags) for a dataset."""
    return await controller.list_branches(dataset_id)


@router.get(
    "/{dataset_id}/branches/{branch_name:path}/head",
    response_model=DatasetVersion,
    summary="Get branch head",
    description="Get the latest version on a branch"
)
async def get_branch_head(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    branch_name: str = Path(..., description="Branch name"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> DatasetVersion:
    """Get the latest version on a specific branch."""
    return await controller.get_branch_head(dataset_id, branch_name)


@router.get(
    "/{dataset_id}/branches/{branch_name:path}/history",
    response_model=List[DatasetVersion],
    summary="Get branch history",
    description="Get commit history for a branch"
)
async def get_branch_history(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    branch_name: str = Path(..., description="Branch name"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[DatasetVersion]:
    """Get the commit history for a branch by following parent links."""
    return await controller.get_branch_history(dataset_id, branch_name)


@router.post(
    "/{dataset_id}/branches/{branch_name:path}/commit",
    response_model=DatasetUploadResponse,
    summary="Commit to branch",
    description="Create a new version on a specific branch"
)
async def commit_to_branch(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    branch_name: str = Path(..., description="Target branch"),
    file: UploadFile = File(..., description="The dataset file to commit"),
    message: Optional[str] = Form(None, description="Commit message"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> DatasetUploadResponse:
    """Create a new version on a specific branch."""
    return await controller.commit_to_branch(dataset_id, branch_name, file, message, current_user)


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
    "/{dataset_id}/permissions/{user_id}/{permission_type}",
    response_model=Dict[str, Any],
    summary="Revoke permission",
    description="Revoke a specific permission from a user"
)
async def revoke_dataset_permission(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    user_id: int = Path(..., description="User ID to revoke permission from"),
    permission_type: DatasetPermissionType = Path(..., description="Permission type to revoke"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> Dict[str, Any]:
    """Revoke permission from a user for this dataset (requires admin permission)."""
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
    
    # Don't allow revoking own admin permission
    if user_id == current_user_id and permission_type == DatasetPermissionType.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot revoke your own admin permission"
        )
    
    success = await controller.service.user_service.revoke_dataset_permission(
        dataset_id,
        user_id,
        permission_type
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Permission not found"
        )
    
    return {"message": f"Permission {permission_type} revoked from user {user_id}"}