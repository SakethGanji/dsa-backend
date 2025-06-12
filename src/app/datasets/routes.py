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
    DatasetVersion, Tag, Sheet
)
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
    repository = DatasetsRepository(session)
    storage_backend = StorageFactory.get_instance()
    service = DatasetsService(repository, storage_backend)
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
        tags=tags
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
        tags=tags
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
    return await controller.get_dataset(dataset_id)


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
    return await controller.update_dataset(dataset_id, data)


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
    await controller.delete_dataset_version(version_id)


@router.get(
    "/{dataset_id}/versions/{version_id}/sheets",
    response_model=List[Sheet],
    summary="List version sheets",
    description="Get all sheets in a dataset version"
)
async def list_version_sheets(
    dataset_id: int = Path(..., gt=0, description="Dataset ID"),
    version_id: int = Path(..., gt=0, description="Version ID"),
    controller: ControllerDep = None,
    current_user: UserDep = None
) -> List[Sheet]:
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