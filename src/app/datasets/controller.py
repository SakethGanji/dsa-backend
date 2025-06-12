from fastapi import HTTPException, status, UploadFile
from typing import List, Optional, Dict, Any
import json
import logging

from app.datasets.service import DatasetsService
from app.datasets.models import (
    DatasetUploadRequest, DatasetUploadResponse, DatasetUpdate, Dataset, DatasetVersion, File, Tag, Sheet
)
from app.datasets.exceptions import (
    DatasetNotFound, DatasetVersionNotFound, FileProcessingError, StorageError
)

logger = logging.getLogger(__name__)

class DatasetsController:
    """Controller layer handling HTTP requests and responses for datasets"""
    
    def __init__(self, service: DatasetsService):
        self.service = service

    def _parse_tags(self, tags: Optional[str]) -> Optional[List[str]]:
        """Parse tags from JSON or comma-separated string"""
        if not tags:
            return None
        try:
            parsed = json.loads(tags)
            if not isinstance(parsed, list):
                raise ValueError("Tags must be a list")
            return parsed
        except json.JSONDecodeError:
            # Fallback to comma-separated format
            return [t.strip() for t in tags.split(',') if t.strip()]
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid tags format: {str(e)}"
            )

    async def upload_dataset(
        self,
        file: UploadFile,
        current_user: Any,
        dataset_id: Optional[int],
        name: str,
        description: Optional[str],
        tags: Optional[str]
    ) -> DatasetUploadResponse:
        """Handle dataset upload request"""
        request = DatasetUploadRequest(
            dataset_id=dataset_id,
            name=name,
            description=description,
            tags=self._parse_tags(tags)
        )
        
        try:
            # Get user ID from the database based on soeid
            user_id = await self.service.get_user_id_from_soeid(current_user.soeid)
            if not user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User not found in the system"
                )
            
            return await self.service.upload_dataset(file, request, user_id)
            
        except (FileProcessingError, StorageError) as e:
            logger.error(f"Dataset upload error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e)
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during dataset upload: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="An unexpected error occurred during upload"
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
        """List datasets with filtering and pagination"""
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
        except Exception as e:
            logger.error(f"Error listing datasets: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve datasets"
            )

    async def get_dataset(self, dataset_id: int) -> Dataset:
        """Get a single dataset by ID"""
        try:
            result = await self.service.get_dataset(dataset_id)
            if not result:
                raise DatasetNotFound(dataset_id)
            return result
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
        except Exception as e:
            logger.error(f"Error retrieving dataset {dataset_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve dataset"
            )

    async def update_dataset(
        self,
        dataset_id: int,
        data: DatasetUpdate
    ) -> Dataset:
        """Update dataset metadata"""
        try:
            updated = await self.service.update_dataset(dataset_id, data)
            if not updated:
                raise DatasetNotFound(dataset_id)
            return updated
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
        except Exception as e:
            logger.error(f"Error updating dataset {dataset_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update dataset"
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
        if not file_info:
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

