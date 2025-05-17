from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import (APIRouter, Body, Depends, File, Form, HTTPException,
                     Path, Query, UploadFile, status)
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, validator # For ListDatasetsQueryParams
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.connection import get_session
# Assuming models are structured as per the comments above
from app.datasets.models import (Dataset, DatasetUpdate, DatasetUploadResponse,
                                 DatasetVersion, Tag)
# These would ideally be in app.datasets.models too
class VersionSheetInfo(BaseModel): # Placeholder
    name: str
class SheetDataResponse(BaseModel): # Placeholder
    headers: List[str]
    rows: List[List[Any]]
    has_more: bool
    offset: int
    limit: int
    total_rows_in_sheet: Optional[int] = None

from app.datasets.controller import DatasetsController
from app.datasets.repository import DatasetsRepository
from app.datasets.service import DatasetsService
from app.users.models import UserOut as User


# Authentication dependency - placeholder
async def get_current_user() -> User:
    return User(
        id=1,
        soeid="mock_user",
        role_id=1,
        created_at=datetime.now(),
        updated_at=datetime.now()
    )

router = APIRouter(prefix="/api/datasets", tags=["Datasets"])

def get_datasets_controller(session: AsyncSession = Depends(get_session)) -> DatasetsController:
    repository = DatasetsRepository(session)
    service = DatasetsService(repository)
    controller = DatasetsController(service)
    return controller

# Pydantic model for list_datasets query parameters
class ListDatasetsQueryParams(BaseModel):
    limit: int = Query(10, ge=1, le=100, description="Maximum number of results to return")
    offset: int = Query(0, ge=0, description="Number of results to skip")
    name: Optional[str] = Query(None, description="Filter by name", min_length=1)
    description: Optional[str] = Query(None, description="Filter by description", min_length=1)
    created_by: Optional[int] = Query(None, description="Filter by creator ID (user ID)")
    tags: Optional[List[str]] = Query(None, description="Filter by tags (comma-separated or multiple query params)")
    sort_by: Optional[str] = Query("created_at", description="Field to sort by (e.g., name, created_at)")
    sort_order: Optional[str] = Query("desc", description="Sort order (asc or desc)", pattern="^(asc|desc)$")
    file_type: Optional[str] = Query(None, description="Filter by file type (e.g., csv, xlsx)")
    file_size_min: Optional[int] = Query(None, description="Minimum file size in bytes", ge=0)
    file_size_max: Optional[int] = Query(None, description="Maximum file size in bytes", ge=0)
    version_min: Optional[int] = Query(None, description="Minimum version number", ge=1)
    version_max: Optional[int] = Query(None, description="Maximum version number", ge=1)
    created_at_from: Optional[datetime] = Query(None, description="Filter datasets created after this ISO datetime")
    created_at_to: Optional[datetime] = Query(None, description="Filter datasets created before this ISO datetime")
    updated_at_from: Optional[datetime] = Query(None, description="Filter datasets updated after this ISO datetime")
    updated_at_to: Optional[datetime] = Query(None, description="Filter datasets updated before this ISO datetime")

    @validator('file_size_max')
    def file_size_max_gte_min(cls, v, values):
        if v is not None and 'file_size_min' in values and values['file_size_min'] is not None:
            if v < values['file_size_min']:
                raise ValueError('file_size_max must be greater than or equal to file_size_min')
        return v

    @validator('version_max')
    def version_max_gte_min(cls, v, values):
        if v is not None and 'version_min' in values and values['version_min'] is not None:
            if v < values['version_min']:
                raise ValueError('version_max must be greater than or equal to version_min')
        return v

    @validator('created_at_to')
    def created_at_to_gte_from(cls, v, values):
        if v is not None and 'created_at_from' in values and values['created_at_from'] is not None:
            if v < values['created_at_from']:
                raise ValueError('created_at_to must be after or same as created_at_from')
        return v

    @validator('updated_at_to')
    def updated_at_to_gte_from(cls, v, values):
        if v is not None and 'updated_at_from' in values and values['updated_at_from'] is not None:
            if v < values['updated_at_from']:
                raise ValueError('updated_at_to must be after or same as updated_at_from')
        return v

@router.post(
    "/upload",
    response_model=DatasetUploadResponse,
    status_code=status.HTTP_201_CREATED
)
async def upload_dataset(
    file: UploadFile = File(...),
    name: str = Form(...),
    description: Optional[str] = Form(None),
    tags: Optional[str] = Form(None, description="Comma-separated tags or JSON array string"),
    dataset_id: Optional[int] = Form(None, description="ID of existing dataset to create a new version for"),
    controller: DatasetsController = Depends(get_datasets_controller),
    current_user: User = Depends(get_current_user)
):
    """
    Upload a new dataset or a new version of an existing dataset.
    If `dataset_id` is provided, creates a new version for that dataset.
    Otherwise, creates a new dataset.
    """
    return await controller.upload_dataset(
        file=file,
        current_user=current_user,
        dataset_id=dataset_id,
        name=name,
        description=description,
        tags_str=tags  # Pass as tags_str to be parsed in controller
    )

@router.get("", response_model=List[Dataset])
async def list_datasets(
    params: ListDatasetsQueryParams = Depends(),
    controller: DatasetsController = Depends(get_datasets_controller),
    current_user: User = Depends(get_current_user) # Auth, can be removed if datasets are public
):
    """
    List datasets with advanced filtering, sorting, and pagination.
    """
    return await controller.list_datasets(
        limit=params.limit,
        offset=params.offset,
        sort_by=params.sort_by,
        sort_order=params.sort_order,
        name=params.name,
        description=params.description,
        created_by=params.created_by,
        tags=params.tags,
        file_type=params.file_type,
        file_size_min=params.file_size_min,
        file_size_max=params.file_size_max,
        version_min=params.version_min,
        version_max=params.version_max,
        created_at_from=params.created_at_from,
        created_at_to=params.created_at_to,
        updated_at_from=params.updated_at_from,
        updated_at_to=params.updated_at_to
    )

@router.get("/tags", response_model=List[Tag])
async def list_tags(
    controller: DatasetsController = Depends(get_datasets_controller),
    # current_user: User = Depends(get_current_user) # Auth if tags are not public
):
    """List all existing tags used across datasets."""
    return await controller.list_tags()

@router.get("/{dataset_id}", response_model=Dataset)
async def get_dataset_details(
    dataset_id: int = Path(..., description="The ID of the dataset to retrieve"),
    controller: DatasetsController = Depends(get_datasets_controller),
    current_user: User = Depends(get_current_user) # Auth for specific dataset
):
    """Get full metadata for a single dataset."""
    return await controller.get_dataset(dataset_id)

@router.patch("/{dataset_id}", response_model=Dataset)
async def update_dataset_metadata(
    dataset_id: int = Path(..., description="The ID of the dataset to update"),
    data: DatasetUpdate = Body(...),
    controller: DatasetsController = Depends(get_datasets_controller),
    current_user: User = Depends(get_current_user) # Auth: user must have rights
):
    """Update dataset metadata (name, description, tags)."""
    # Add permission check here if needed: e.g., is current_user owner or admin?
    return await controller.update_dataset(dataset_id, data, current_user.id)

@router.get("/{dataset_id}/versions", response_model=List[DatasetVersion])
async def list_dataset_versions(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    controller: DatasetsController = Depends(get_datasets_controller),
    current_user: User = Depends(get_current_user) # Auth
):
    """List all versions for a specific dataset."""
    return await controller.list_dataset_versions(dataset_id)

@router.get("/{dataset_id}/versions/{version_id}", response_model=DatasetVersion)
async def get_dataset_version_details(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    controller: DatasetsController = Depends(get_datasets_controller),
    current_user: User = Depends(get_current_user) # Auth
):
    """Get metadata for a single dataset version."""
    return await controller.get_dataset_version(dataset_id=dataset_id, version_id=version_id)

@router.get("/{dataset_id}/versions/{version_id}/download")
async def download_dataset_version_file(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    controller: DatasetsController = Depends(get_datasets_controller),
    current_user: User = Depends(get_current_user) # Auth
):
    """Stream-download the raw file for a specific dataset version."""
    file_info = await controller.get_dataset_version_file_data(dataset_id=dataset_id, version_id=version_id)

    def iter_file():
        yield file_info.file_data

    return StreamingResponse(
        iter_file(),
        media_type=file_info.mime_type,
        headers={
            "Content-Disposition": f"attachment; filename=\"{file_info.original_file_name}\""
        }
    )

@router.delete(
    "/{dataset_id}/versions/{version_id}",
    status_code=status.HTTP_204_NO_CONTENT
)
async def delete_dataset_version(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version to delete"),
    controller: DatasetsController = Depends(get_datasets_controller),
    current_user: User = Depends(get_current_user) # Auth: user must have rights
):
    """
    Delete a specific dataset version.
    Ensure the user has permission to delete.
    """
    # Add permission check here if needed
    await controller.delete_dataset_version(dataset_id=dataset_id, version_id=version_id, user_id=current_user.id)
    return None # For 204 No Content

@router.get("/{dataset_id}/versions/{version_id}/sheets", response_model=List[VersionSheetInfo])
async def list_version_sheets(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    controller: DatasetsController = Depends(get_datasets_controller),
    current_user: User = Depends(get_current_user) # Auth
):
    """List all sheets (e.g., in an Excel file) in a dataset version."""
    return await controller.list_version_sheets(dataset_id=dataset_id, version_id=version_id)

@router.get("/{dataset_id}/versions/{version_id}/data", response_model=SheetDataResponse)
async def get_version_sheet_data(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    sheet_name: Optional[str] = Query(None, description="Sheet name to get data from. For CSV, this is optional/ignored."),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of rows to return"),
    offset: int = Query(0, ge=0, description="Number of rows to skip"),
    controller: DatasetsController = Depends(get_datasets_controller),
    current_user: User = Depends(get_current_user) # Auth
):
    """Get paginated data from a sheet in a dataset version."""
    return await controller.get_sheet_data(
        dataset_id=dataset_id,
        version_id=version_id,
        sheet_name=sheet_name,
        limit=limit,
        offset=offset
    )