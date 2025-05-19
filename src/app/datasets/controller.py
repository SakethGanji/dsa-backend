from fastapi import HTTPException, status, UploadFile
from typing import List, Optional, Dict, Any
import json

from app.datasets.service import DatasetsService
from app.datasets.models import (
    DatasetUploadRequest, DatasetUploadResponse, DatasetUpdate, Dataset, DatasetVersion, File, Tag, Sheet
)

class DatasetsController:
    def __init__(self, service: DatasetsService):
        self.service = service

    def _parse_tags(self, tags: Optional[str]) -> Optional[List[str]]:
        if not tags:
            return None
        try:
            parsed = json.loads(tags)
            if not isinstance(parsed, list):
                raise ValueError
            return parsed
        except json.JSONDecodeError:
            # comma-separated fallback
            return [t.strip() for t in tags.split(',') if t.strip()]
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tags must be a JSON list or comma-separated string"
            )

    async def upload_dataset(
        self,
        file: UploadFile,
        current_user: Any, # This will be a CurrentUser object from JWT
        dataset_id: Optional[int],
        name: str,
        description: Optional[str],
        tags: Optional[str]
    ) -> DatasetUploadResponse:
        request = DatasetUploadRequest(
            dataset_id=dataset_id,
            name=name,
            description=description,
            tags=self._parse_tags(tags)
        )
        try:
            # Map user attributes from CurrentUser (JWT token) to database user
            # For database operations, we need a user ID
            # In this example, we'll use the SOEID as a unique identifier
            # In a real app, you would have a users table with IDs
            user_id = 1  # Mock user ID
            
            # You could also implement role-based access control here:
            # if not current_user.is_admin():
            #     raise HTTPException(
            #         status_code=status.HTTP_403_FORBIDDEN,
            #         detail="Only admins can upload datasets"
            #     )
            
            return await self.service.upload_dataset(file, request, user_id)
        except HTTPException: # Re-raise HTTPException
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Upload failed: {str(e)}"
            )

    async def list_datasets(
        self,
        limit: int,
        offset: int,
        name: Optional[str],
        description: Optional[str],
        created_by: Optional[int],
        tags: Optional[List[str]],
        sort_by: Optional[str],
        sort_order: Optional[str]
    ) -> List[Dataset]:
        try:
            return await self.service.list_datasets(
                limit=limit,
                offset=offset,
                name=name,
                description=description,
                created_by=created_by,
                tags=tags,
                sort_by=sort_by,
                sort_order=sort_order
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Listing datasets failed: {e}"
            )

    async def get_dataset(self, dataset_id: int) -> Dataset:
        result = await self.service.get_dataset(dataset_id)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
        return result

    async def update_dataset(
        self,
        dataset_id: int,
        data: DatasetUpdate
    ) -> Dataset:
        try:
            updated = await self.service.update_dataset(dataset_id, data)
            if not updated:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Dataset {dataset_id} not found"
                )
            return updated
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Update failed: {e}"
            )

    async def list_dataset_versions(self, dataset_id: int) -> List[DatasetVersion]:
        return await self.service.list_dataset_versions(dataset_id)

    async def get_dataset_version(self, version_id: int) -> DatasetVersion:
        version = await self.service.get_dataset_version(version_id)
        if not version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {version_id} not found"
            )
        return version

    async def get_dataset_version_file(self, version_id: int) -> File:
        file_info = await self.service.get_dataset_version_file(version_id)
        if not file_info or not file_info.file_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File for version {version_id} not found"
            )
        return file_info

    async def delete_dataset_version(self, version_id: int) -> None:
        success = await self.service.delete_dataset_version(version_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {version_id} not found"
            )

    async def list_tags(self) -> List[Tag]:
        return await self.service.list_tags()

    async def list_version_sheets(self, version_id: int) -> List[Sheet]:
        sheets = await self.service.list_version_sheets(version_id)
        # The service already handles the case where the version doesn't exist and returns [].
        # If sheets is an empty list, it will be returned as such, which is appropriate.
        return sheets

    async def get_sheet_data(
        self,
        version_id: int,
        sheet_name: Optional[str],
        limit: int,
        offset: int
    ) -> Dict[str, Any]:
        headers, rows, has_more = await self.service.get_sheet_data(
            version_id=version_id,
            sheet_name=sheet_name,
            limit=limit,
            offset=offset
        )
        if not headers:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Sheet not found or invalid"
            )
        return {
            "headers": headers,
            "rows": rows,
            "has_more": has_more,
            "offset": offset,
            "limit": limit,
            "total": None
        }

    async def get_version_for_dataset(self, dataset_id: int, version_id: int) -> DatasetVersion:
        version = await self.get_dataset_version(version_id)
        if version.dataset_id != dataset_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {version_id} does not belong to dataset {dataset_id}"
            )
        return version

