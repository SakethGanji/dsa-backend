import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, UploadFile, status

from app.datasets.models import (Dataset, DatasetCreate, DatasetUpdate,
                                 DatasetUploadRequest, DatasetUploadResponse,
                                 DatasetVersion, Tag)
# These would ideally be in app.datasets.models too
from pydantic import BaseModel  # For FileInfoResponse, VersionSheetInfo, SheetDataResponse


class FileInfoResponse(BaseModel):
    file_data: bytes
    file_type_extension: str
    mime_type: str
    original_file_name: str


class VersionSheetInfo(BaseModel):
    name: str


class SheetDataResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    has_more: bool
    offset: int
    limit: int
    total_rows_in_sheet: Optional[int] = None


from app.datasets.service import DatasetsService
from app.users.models import UserOut as User

class DatasetsController:
    def __init__(self, service: DatasetsService):
        self.service = service

    async def _handle_service_error(self, operation: str, error: Exception):
        """Centralized error handling for service operations."""
        if isinstance(error, HTTPException):  # Re-raise FastAPI HTTPExceptions
            raise error
        # Log the error for debugging: logger.error(f"Service error during {operation}: {error}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while {operation}. Please try again later."
        )

    async def _get_existing_dataset_or_404(self, dataset_id: int) -> Dataset:
        """Fetch a dataset by ID or raise HTTPException 404."""
        dataset = await self.service.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset with ID {dataset_id} not found"
            )
        return dataset  # Should be Pydantic model Dataset

    async def _get_existing_version_or_404(self, version_id: int) -> DatasetVersion:
        """Fetch a version by ID or raise HTTPException 404."""
        version = await self.service.get_dataset_version(version_id)
        if not version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset version with ID {version_id} not found"
            )
        return version  # Should be Pydantic model DatasetVersion

    async def _check_version_belongs_to_dataset(self, version: DatasetVersion, dataset_id: int):
        """Check if a version belongs to the given dataset_id, raise 404 if not."""
        if version.dataset_id != dataset_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {version.id} does not belong to dataset {dataset_id}"
            )

    async def upload_dataset(
            self,
            file: UploadFile,
            current_user: User,
            name: str,
            description: Optional[str] = None,
            tags_str: Optional[str] = None,
            dataset_id: Optional[int] = None
    ) -> DatasetUploadResponse:
        """
        Upload a new dataset or a new version of an existing dataset.
        `tags_str` is a string that can be comma-separated or a JSON array string.
        """
        parsed_tags: Optional[List[str]] = None
        if tags_str:
            try:
                # Attempt to parse as JSON array first
                data = json.loads(tags_str)
                if isinstance(data, list) and all(isinstance(tag, str) for tag in data):
                    parsed_tags = data
                else:
                    raise ValueError("JSON was not a list of strings.")
            except (json.JSONDecodeError, ValueError):
                # Fallback to comma-separated string
                parsed_tags = [tag.strip() for tag in tags_str.split(',') if tag.strip()]

            if not parsed_tags and tags_str.strip():  # If parsing resulted in empty but original wasn't empty
                parsed_tags = [tags_str.strip()]  # Treat as a single tag if not empty after failed parsing

        upload_request_data = DatasetUploadRequest(
            dataset_id=dataset_id,
            name=name,
            description=description,
            tags=parsed_tags
        )

        try:
            # If dataset_id is provided, verify it exists
            if dataset_id:
                await self._get_existing_dataset_or_404(dataset_id)

            result = await self.service.upload_dataset(
                file=file,
                request=upload_request_data,
                user_id=current_user.id
            )
            return result  # Should be DatasetUploadResponse
        except Exception as e:
            await self._handle_service_error(f"uploading dataset '{name}'", e)

    async def list_datasets(
            self,
            limit: int,
            offset: int,
            sort_by: Optional[str],
            sort_order: Optional[str],
            name: Optional[str],
            description: Optional[str],
            created_by: Optional[int],
            tags: Optional[List[str]],
            file_type: Optional[str],
            file_size_min: Optional[int],
            file_size_max: Optional[int],
            version_min: Optional[int],
            version_max: Optional[int],
            created_at_from: Optional[datetime],
            created_at_to: Optional[datetime],
            updated_at_from: Optional[datetime],
            updated_at_to: Optional[datetime]
    ) -> List[Dataset]:
        """List datasets with filtering, sorting, and pagination."""
        try:
            # Datetime parsing is now handled by Pydantic in ListDatasetsQueryParams
            datasets = await self.service.list_datasets(
                limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order,
                name=name, description=description, created_by=created_by, tags=tags,
                file_type=file_type, file_size_min=file_size_min, file_size_max=file_size_max,
                version_min=version_min, version_max=version_max,
                created_at_from=created_at_from, created_at_to=created_at_to,
                updated_at_from=updated_at_from, updated_at_to=updated_at_to
            )
            return datasets  # Should be List[Dataset]
        except Exception as e:
            await self._handle_service_error("listing datasets", e)

    async def get_dataset(self, dataset_id: int) -> Dataset:
        """Get detailed information about a single dataset."""
        try:
            return await self._get_existing_dataset_or_404(dataset_id)
        except Exception as e:
            await self._handle_service_error(f"getting dataset ID {dataset_id}", e)

    async def update_dataset(self, dataset_id: int, data: DatasetUpdate, user_id: int) -> Dataset:
        """Update dataset metadata. Ensure user has permissions."""
        try:
            # Permission check logic might be here or in service.
            # For now, assume service handles if user_id can update dataset_id.
            # First, check if dataset exists
            await self._get_existing_dataset_or_404(dataset_id)

            updated_dataset = await self.service.update_dataset(dataset_id, data, user_id)
            if not updated_dataset:  # Should not happen if get_dataset passed, but as safeguard
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="Dataset not found after update attempt.")
            return updated_dataset  # Should be Dataset
        except Exception as e:
            await self._handle_service_error(f"updating dataset ID {dataset_id}", e)

    async def list_dataset_versions(self, dataset_id: int) -> List[DatasetVersion]:
        """List all versions of a dataset."""
        try:
            await self._get_existing_dataset_or_404(dataset_id)  # Ensure dataset exists
            versions = await self.service.list_dataset_versions(dataset_id)
            return versions  # Should be List[DatasetVersion]
        except Exception as e:
            await self._handle_service_error(f"listing versions for dataset ID {dataset_id}", e)

    async def get_dataset_version(self, dataset_id: int, version_id: int) -> DatasetVersion:
        """Get detailed information about a single dataset version, ensuring it belongs to the dataset."""
        try:
            # Dataset existence check implicitly done by version check if service links them
            # or explicitly: await self._get_existing_dataset_or_404(dataset_id)
            version = await self._get_existing_version_or_404(version_id)
            await self._check_version_belongs_to_dataset(version, dataset_id)
            return version
        except Exception as e:
            await self._handle_service_error(f"getting version ID {version_id} for dataset ID {dataset_id}", e)

    async def get_dataset_version_file_data(self, dataset_id: int, version_id: int) -> FileInfoResponse:
        """Get file data and metadata for a dataset version."""
        try:
            version = await self._get_existing_version_or_404(version_id)
            await self._check_version_belongs_to_dataset(version, dataset_id)

            # Service method should return a dict or a Pydantic model with file_data, file_type, mime_type, file_name
            file_info_dict = await self.service.get_dataset_version_file(version_id)
            if not file_info_dict or "file_data" not in file_info_dict:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "File data not found for this version.")

            return FileInfoResponse(
                file_data=file_info_dict["file_data"],
                file_type_extension=file_info_dict.get("file_type", "bin"),  # e.g. csv, xlsx
                mime_type=file_info_dict.get("mime_type", "application/octet-stream"),
                original_file_name=file_info_dict.get("file_name", f"{version.file_name}")
                # or construct based on version
            )
        except Exception as e:
            await self._handle_service_error(f"downloading file for version ID {version_id}", e)

    async def delete_dataset_version(self, dataset_id: int, version_id: int, user_id: int) -> None:
        """Delete a dataset version. Ensure user has permissions."""
        try:
            # Permission check logic might be here or in service
            version = await self._get_existing_version_or_404(version_id)
            await self._check_version_belongs_to_dataset(version, dataset_id)

            success = await self.service.delete_dataset_version(version_id, user_id)
            if not success:  # If service returns boolean for success
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to delete version.")
            # If service raises exception on failure, this is not needed.
        except Exception as e:
            await self._handle_service_error(f"deleting version ID {version_id}", e)

    async def list_tags(self) -> List[Tag]:
        """List all available tags."""
        try:
            tags = await self.service.list_tags()
            return tags  # Should be List[Tag]
        except Exception as e:
            await self._handle_service_error("listing tags", e)

    async def list_version_sheets(self, dataset_id: int, version_id: int) -> List[VersionSheetInfo]:
        """Get all sheet names for a dataset version (e.g., for Excel files)."""
        try:
            version = await self._get_existing_version_or_404(version_id)
            await self._check_version_belongs_to_dataset(version, dataset_id)

            # Service method should return list of dicts or VersionSheetInfo models
            sheets_data = await self.service.list_version_sheets(version_id)
            return [VersionSheetInfo(**s) if isinstance(s, dict) else s for s in sheets_data]
        except Exception as e:
            await self._handle_service_error(f"listing sheets for version ID {version_id}", e)

    async def get_sheet_data(
            self,
            dataset_id: int,
            version_id: int,
            sheet_name: Optional[str],
            limit: int,
            offset: int
    ) -> SheetDataResponse:
        """Get paginated data from a sheet within a dataset version."""
        try:
            version = await self._get_existing_version_or_404(version_id)
            await self._check_version_belongs_to_dataset(version, dataset_id)

            # Service method returns tuple: (headers, rows, has_more, total_rows_in_sheet (optional))
            result = await self.service.get_sheet_data(
                version_id=version_id, sheet_name=sheet_name, limit=limit, offset=offset
            )
            # Unpack based on what service.get_sheet_data returns
            # Assuming it's a dict or a Pydantic model for now.
            # If it's (headers, rows, has_more, total_rows_in_sheet)
            # headers, data_rows, has_more, total_rows = result

            # For this example, let's assume service.get_sheet_data returns a dict matching SheetDataResponse structure
            # or that we construct it here.
            # This is a simplified example of how service might return data
            if isinstance(result, tuple) and len(result) >= 3:
                headers, data_rows, has_more = result[:3]
                total_rows = result[3] if len(result) > 3 else None
            elif isinstance(result, dict):  # if service returns a dict
                headers = result.get("headers", [])
                data_rows = result.get("rows", [])
                has_more = result.get("has_more", False)
                total_rows = result.get("total_rows_in_sheet")
            else:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                    detail="Invalid data format from service for sheet data.")

            if not headers and not data_rows:  # No data or sheet not found
                # This check might be more specific based on service layer's behavior for non-existent sheets
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Sheet '{sheet_name or 'default'}' not found or is empty in version ID {version_id}."
                )

            return SheetDataResponse(
                headers=headers,
                rows=data_rows,
                has_more=has_more,
                offset=offset,
                limit=limit,
                total_rows_in_sheet=total_rows
            )
        except Exception as e:
            await self._handle_service_error(f"getting sheet data for version ID {version_id}", e)