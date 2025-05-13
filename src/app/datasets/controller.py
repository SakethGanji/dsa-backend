from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, status
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
from app.datasets.service import DatasetsService
from app.datasets.models import (
    Dataset, DatasetCreate, DatasetUpdate, DatasetUploadRequest, DatasetUploadResponse
)
from app.users.models import UserOut as User  # Using UserOut as User

class DatasetsController:
    def __init__(self, service: DatasetsService):
        self.service = service
    
    async def upload_dataset(
        self,
        file: UploadFile,
        current_user: User,
        dataset_id: Optional[int] = None,
        name: str = "",
        description: Optional[str] = None,
        tags: Optional[str] = None
    ) -> DatasetUploadResponse:
        """
        Upload a new dataset or a new version of an existing dataset
        """
        # Parse tags from JSON string if provided
        parsed_tags = None
        if tags:
            try:
                # First, try to parse as JSON
                parsed_tags = json.loads(tags)
            except json.JSONDecodeError:
                # If that fails, try to handle common formatting issues
                try:
                    # Handle case where tags might be comma-separated
                    if ',' in tags:
                        parsed_tags = [tag.strip() for tag in tags.split(',')]
                    # Handle case where it's a single tag
                    elif tags.strip():
                        parsed_tags = [tags.strip()]
                    else:
                        parsed_tags = []
                except Exception:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Invalid tags format. Expected JSON array or comma-separated values."
                    )

            # Ensure parsed_tags is a list
            if not isinstance(parsed_tags, list):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Tags must be provided as a list/array."
                )
        
        request = DatasetUploadRequest(
            dataset_id=dataset_id,
            name=name,
            description=description,
            tags=parsed_tags
        )
        
        try:
            result = await self.service.upload_dataset(
                file=file,
                request=request,
                user_id=current_user.id
            )
            return result
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to upload dataset: {str(e)}"
            )
    
    # Controller methods for dataset operations
    async def _handle_service_error(self, operation: str, error: Exception) -> None:
        """Centralized error handling for service operations"""
        if isinstance(error, HTTPException):
            raise error
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to {operation}: {str(error)}"
            )

    async def list_datasets(
        self,
        limit: int = 10,
        offset: int = 0,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        created_by: Optional[int] = None,
        tags: Optional[List[str]] = None,
        file_type: Optional[str] = None,
        file_size_min: Optional[int] = None,
        file_size_max: Optional[int] = None,
        version_min: Optional[int] = None,
        version_max: Optional[int] = None,
        created_at_from: Optional[str] = None,
        created_at_to: Optional[str] = None,
        updated_at_from: Optional[str] = None,
        updated_at_to: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List datasets with optional filtering, sorting, and pagination"""
        try:
            # Parse datetime strings if provided
            created_at_from_dt = None
            created_at_to_dt = None
            updated_at_from_dt = None
            updated_at_to_dt = None

            try:
                if created_at_from:
                    created_at_from_dt = datetime.fromisoformat(created_at_from.replace('Z', '+00:00'))
                if created_at_to:
                    created_at_to_dt = datetime.fromisoformat(created_at_to.replace('Z', '+00:00'))
                if updated_at_from:
                    updated_at_from_dt = datetime.fromisoformat(updated_at_from.replace('Z', '+00:00'))
                if updated_at_to:
                    updated_at_to_dt = datetime.fromisoformat(updated_at_to.replace('Z', '+00:00'))
            except ValueError as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid datetime format: {str(e)}"
                )

            return await self.service.list_datasets(
                limit=limit,
                offset=offset,
                sort_by=sort_by,
                sort_order=sort_order,
                name=name,
                description=description,
                created_by=created_by,
                tags=tags,
                file_type=file_type,
                file_size_min=file_size_min,
                file_size_max=file_size_max,
                version_min=version_min,
                version_max=version_max,
                created_at_from=created_at_from_dt,
                created_at_to=created_at_to_dt,
                updated_at_from=updated_at_from_dt,
                updated_at_to=updated_at_to_dt
            )
        except Exception as e:
            await self._handle_service_error("list datasets", e)


    async def _check_resource_exists(self, resource_id: int, get_method, resource_type: str) -> Dict[str, Any]:
        """Check if a resource exists and raise 404 if not"""
        try:
            resource = await get_method(resource_id)
            if not resource:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"{resource_type} with ID {resource_id} not found"
                )
            return resource
        except Exception as e:
            await self._handle_service_error(f"get {resource_type.lower()}", e)

    async def get_dataset(self, dataset_id: int) -> Dict[str, Any]:
        """Get detailed information about a single dataset"""
        return await self._check_resource_exists(
            dataset_id, self.service.get_dataset, "Dataset"
        )

    async def update_dataset(self, dataset_id: int, data: DatasetUpdate) -> Dict[str, Any]:
        """Update dataset metadata"""
        try:
            updated_dataset = await self.service.update_dataset(dataset_id, data)
            if not updated_dataset:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Dataset with ID {dataset_id} not found"
                )
            return updated_dataset
        except Exception as e:
            await self._handle_service_error("update dataset", e)

    async def list_dataset_versions(self, dataset_id: int) -> List[Dict[str, Any]]:
        """List all versions of a dataset"""
        try:
            # This will return an empty list if dataset doesn't exist
            versions = await self.service.list_dataset_versions(dataset_id)
            if not versions:
                # Check if dataset exists
                await self._check_resource_exists(
                    dataset_id, self.service.get_dataset, "Dataset"
                )
            return versions
        except Exception as e:
            await self._handle_service_error("list dataset versions", e)

    async def get_dataset_version(self, version_id: int) -> Dict[str, Any]:
        """Get detailed information about a single dataset version"""
        return await self._check_resource_exists(
            version_id, self.service.get_dataset_version, "Dataset version"
        )

    async def get_dataset_version_file(self, version_id: int):
        """Get file for a dataset version"""
        try:
            # Verify version exists first
            await self._check_resource_exists(
                version_id, self.service.get_dataset_version, "Dataset version"
            )

            file_info = await self.service.get_dataset_version_file(version_id)
            if not file_info:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"File for dataset version with ID {version_id} not found"
                )
            return file_info
        except Exception as e:
            await self._handle_service_error("get dataset version file", e)

    async def delete_dataset_version(self, version_id: int) -> Dict[str, bool]:
        """Delete a dataset version"""
        try:
            # Verify version exists first
            await self._check_resource_exists(
                version_id, self.service.get_dataset_version, "Dataset version"
            )

            success = await self.service.delete_dataset_version(version_id)
            return {"ok": True}
        except Exception as e:
            await self._handle_service_error("delete dataset version", e)

    async def list_tags(self) -> List[Dict[str, Any]]:
        """List all available tags"""
        try:
            return await self.service.list_tags()
        except Exception as e:
            await self._handle_service_error("list tags", e)

    async def list_version_sheets(self, version_id: int) -> List[Dict[str, Any]]:
        """Get all sheets for a dataset version"""
        try:
            # Verify version exists first
            await self._check_resource_exists(
                version_id, self.service.get_dataset_version, "Dataset version"
            )

            sheets = await self.service.list_version_sheets(version_id)
            return sheets
        except Exception as e:
            await self._handle_service_error("list sheets", e)

    async def get_sheet_data(self, version_id: int, sheet_name: Optional[str], limit: int, offset: int) -> Dict[str, Any]:
        """Get paginated data from a sheet"""
        try:
            # Verify version exists first
            await self._check_resource_exists(
                version_id, self.service.get_dataset_version, "Dataset version"
            )

            # Get sheet data
            headers, rows, has_more = await self.service.get_sheet_data(
                version_id=version_id,
                sheet_name=sheet_name,
                limit=limit,
                offset=offset
            )

            if not headers:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Sheet not found or couldn't be processed"
                )

            return {
                "headers": headers,
                "rows": rows,
                "has_more": has_more,
                "offset": offset,
                "limit": limit,
                "total": None  # We don't know the total without scanning the whole file
            }
        except Exception as e:
            await self._handle_service_error("get sheet data", e)