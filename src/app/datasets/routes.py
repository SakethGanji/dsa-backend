from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Body, Query, Path, status
from fastapi.responses import StreamingResponse
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from app.datasets.controller import DatasetsController
from app.datasets.service import DatasetsService
from app.datasets.repository import DatasetsRepository
from app.datasets.models import (
    Dataset, DatasetCreate, DatasetUploadResponse, DatasetUpdate,
    DatasetVersion, Tag, DatasetListParams
)
from app.users.models import UserOut as User
from app.db.connection import get_session
import io


# Authentication dependency - needs to be implemented or imported
# This is a placeholder - you'll need to implement proper auth
async def get_current_user():
    # This is a mock implementation
    # Replace with your actual authentication logic
    return User(
        id=1,
        soeid="mock_user",
        role_id=1,
        created_at=datetime.now(),
        updated_at=datetime.now()
    )


router = APIRouter(prefix="/api/datasets", tags=["Datasets"])


def get_datasets_controller(session: AsyncSession = Depends(get_session)):
    repository = DatasetsRepository(session)
    service = DatasetsService(repository)
    controller = DatasetsController(service)
    return controller


@router.post("/upload", response_model=DatasetUploadResponse)
async def upload_dataset(
        file: UploadFile = File(...),
        dataset_id: Optional[int] = Form(None),
        name: str = Form(...),
        description: Optional[str] = Form(None),
        tags: Optional[str] = Form(None),
        controller: DatasetsController = Depends(get_datasets_controller),
        current_user: User = Depends(get_current_user)
):
    """
    Upload a new dataset or a new version of an existing dataset
    """
    return await controller.upload_dataset(
        file=file,
        current_user=current_user,
        dataset_id=dataset_id,
        name=name,
        description=description,
        tags=tags
    )


# Routes for listing and retrieving datasets
@router.get("", response_model=List[Dataset])
async def list_datasets(
        limit: int = Query(10, ge=1, le=100, description="Maximum number of results to return"),
        offset: int = Query(0, ge=0, description="Number of results to skip"),
        name: Optional[str] = Query(None, description="Filter by name"),
        description: Optional[str] = Query(None, description="Filter by description"),
        created_by: Optional[int] = Query(None, description="Filter by creator ID"),
        tags: Optional[List[str]] = Query(None, description="Filter by tags"),
        sort_by: Optional[str] = Query(None, description="Field to sort by"),
        sort_order: Optional[str] = Query(None, description="Sort order (asc or desc)"),
        controller: DatasetsController = Depends(get_datasets_controller),
        current_user: User = Depends(get_current_user)
):
    """List datasets with optional filtering"""
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


@router.get("/tags", response_model=List[Tag])
async def list_tags(
        controller: DatasetsController = Depends(get_datasets_controller),
        current_user: User = Depends(get_current_user)
):
    """List all existing tags"""
    return await controller.list_tags()


@router.get("/{dataset_id}", response_model=Dataset)
async def get_dataset(
        dataset_id: int = Path(..., description="The ID of the dataset to retrieve"),
        controller: DatasetsController = Depends(get_datasets_controller),
        current_user: User = Depends(get_current_user)
):
    """Get full metadata for one dataset"""
    return await controller.get_dataset(dataset_id)


@router.patch("/{dataset_id}", response_model=Dataset)
async def update_dataset(
        dataset_id: int = Path(..., description="The ID of the dataset to update"),
        data: DatasetUpdate = Body(...),
        controller: DatasetsController = Depends(get_datasets_controller),
        current_user: User = Depends(get_current_user)
):
    """Update dataset metadata (name, description, tags)"""
    return await controller.update_dataset(dataset_id, data)


@router.get("/{dataset_id}/versions", response_model=List[Dict[str, Any]])
async def list_dataset_versions(
        dataset_id: int = Path(..., description="The ID of the dataset"),
        controller: DatasetsController = Depends(get_datasets_controller),
        current_user: User = Depends(get_current_user)
):
    """List all versions for a dataset"""
    return await controller.list_dataset_versions(dataset_id)


@router.get("/{dataset_id}/versions/{version_id}", response_model=Dict[str, Any])
async def get_dataset_version(
        dataset_id: int = Path(..., description="The ID of the dataset"),
        version_id: int = Path(..., description="The ID of the version"),
        controller: DatasetsController = Depends(get_datasets_controller),
        current_user: User = Depends(get_current_user)
):
    """Get metadata for a single version"""
    return await controller.get_version_for_dataset(dataset_id, version_id)


@router.get("/{dataset_id}/versions/{version_id}/download")
async def download_dataset_version(
        dataset_id: int = Path(..., description="The ID of the dataset"),
        version_id: int = Path(..., description="The ID of the version"),
        controller: DatasetsController = Depends(get_datasets_controller),
        current_user: User = Depends(get_current_user)
):
    """Stream-download the raw file for that version"""
    version = await controller.get_version_for_dataset(dataset_id, version_id)
    file_info = await controller.get_dataset_version_file(version_id)
    if not file_info or not file_info.get("file_data"):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File data not found"
        )
    file_data = file_info["file_data"]
    file_type = file_info["file_type"]
    file_name = f"{version['dataset_id']}_v{version['version_number']}.{file_type}"
    media_type = file_info["mime_type"] or "application/octet-stream"
    def iter_file():
        yield file_data
    return StreamingResponse(
        iter_file(),
        media_type=media_type,
        headers={
            "Content-Disposition": f"attachment; filename={file_name}"
        }
    )


@router.delete("/{dataset_id}/versions/{version_id}")
async def delete_dataset_version(
        dataset_id: int = Path(..., description="The ID of the dataset"),
        version_id: int = Path(..., description="The ID of the version"),
        controller: DatasetsController = Depends(get_datasets_controller),
        current_user: User = Depends(get_current_user)
):
    """Delete one specific dataset version"""
    await controller.get_version_for_dataset(dataset_id, version_id)
    return await controller.delete_dataset_version(version_id)


@router.get("/{dataset_id}/versions/{version_id}/sheets")
async def list_version_sheets(
        dataset_id: int = Path(..., description="The ID of the dataset"),
        version_id: int = Path(..., description="The ID of the version"),
        controller: DatasetsController = Depends(get_datasets_controller),
        current_user: User = Depends(get_current_user)
):
    """List all sheets in a dataset version"""
    await controller.get_version_for_dataset(dataset_id, version_id)
    sheets = await controller.list_version_sheets(version_id)
    return sheets


@router.get("/{dataset_id}/versions/{version_id}/data")
async def get_sheet_data(
        dataset_id: int = Path(..., description="The ID of the dataset"),
        version_id: int = Path(..., description="The ID of the version"),
        sheet: Optional[str] = Query(None, description="Sheet name to get data from. For CSV files, this is optional."),
        limit: int = Query(100, ge=1, le=1000, description="Maximum number of rows to return"),
        offset: int = Query(0, ge=0, description="Number of rows to skip"),
        controller: DatasetsController = Depends(get_datasets_controller),
        current_user: User = Depends(get_current_user)
):
    """Get paginated data from a sheet in a dataset version"""
    await controller.get_version_for_dataset(dataset_id, version_id)
    data = await controller.get_sheet_data(
        version_id=version_id,
        sheet_name=sheet,
        limit=limit,
        offset=offset
    )
    return data

