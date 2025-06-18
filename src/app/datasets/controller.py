from fastapi import HTTPException, status, UploadFile
from typing import List, Optional, Dict, Any
import json
import logging

from app.datasets.service import DatasetsService
from app.datasets.models import (
    DatasetUploadRequest, DatasetUploadResponse, DatasetUpdate, Dataset, DatasetVersion, File, Tag, Sheet, SchemaVersion, VersionFile, DatasetPointer
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
        tags: Optional[str],
        parent_version_id: Optional[int] = None,
        message: Optional[str] = None
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
            
            return await self.service.upload_dataset(
                file, request, user_id,
                parent_version_id=parent_version_id,
                message=message
            )
            
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
        sort_order: Optional[str],
        current_user: Any = None
    ) -> List[Dataset]:
        """List datasets with filtering and pagination"""
        try:
            # Get all datasets first
            datasets = await self.service.list_datasets(
                limit=limit,
                offset=offset,
                name=name,
                description=description,
                created_by=created_by,
                tags=tags,
                sort_by=sort_by,
                sort_order=sort_order
            )
            
            # If user is provided, filter by permissions
            if current_user:
                user_id = await self.service.get_user_id_from_soeid(current_user.soeid)
                if user_id:
                    # Filter datasets where user has at least read permission
                    filtered_datasets = []
                    for dataset in datasets:
                        has_permission = await self.service.check_dataset_permission(
                            dataset.id, user_id, "read"
                        )
                        if has_permission:
                            filtered_datasets.append(dataset)
                    return filtered_datasets
            
            return datasets
        except Exception as e:
            logger.error(f"Error listing datasets: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve datasets"
            )

    async def get_dataset(self, dataset_id: int, current_user: Any = None) -> Dataset:
        """Get a single dataset by ID"""
        try:
            # Check read permission if user provided
            if current_user:
                user_id = await self.service.get_user_id_from_soeid(current_user.soeid)
                if user_id:
                    has_permission = await self.service.check_dataset_permission(dataset_id, user_id, "read")
                    if not has_permission:
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail="Read permission required to view dataset"
                        )
            
            result = await self.service.get_dataset(dataset_id)
            if not result:
                raise DatasetNotFound(dataset_id)
            return result
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving dataset {dataset_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve dataset"
            )

    async def update_dataset(
        self,
        dataset_id: int,
        data: DatasetUpdate,
        current_user: Any
    ) -> Dataset:
        """Update dataset metadata"""
        try:
            # Check permission
            user_id = await self.service.get_user_id_from_soeid(current_user.soeid)
            if not user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User not found"
                )
            
            has_permission = await self.service.check_dataset_permission(dataset_id, user_id, "write")
            if not has_permission:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Write permission required to update dataset"
                )
            
            updated = await self.service.update_dataset(dataset_id, data)
            if not updated:
                raise DatasetNotFound(dataset_id)
            return updated
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating dataset {dataset_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update dataset"
            )

    async def list_dataset_versions(self, dataset_id: int) -> List[DatasetVersion]:
        return await self.service.list_dataset_versions(dataset_id)
    
    async def get_version_tree(self, dataset_id: int) -> Dict[str, Any]:
        """Get version tree structure for a dataset"""
        try:
            return await self.service.get_version_tree(dataset_id)
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )

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

    async def delete_dataset_version(self, version_id: int, current_user: Any) -> None:
        # Get version to check dataset
        version = await self.service.get_dataset_version(version_id)
        if not version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {version_id} not found"
            )
        
        # Check write permission
        user_id = await self.service.get_user_id_from_soeid(current_user.soeid)
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not found"
            )
        
        has_permission = await self.service.check_dataset_permission(version.dataset_id, user_id, "write")
        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Write permission required to delete version"
            )
        
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
    
    async def get_schema_for_version(self, version_id: int) -> SchemaVersion:
        """Get schema for a dataset version"""
        try:
            schema = await self.service.get_schema_for_version(version_id)
            if not schema:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Schema not found for version {version_id}"
                )
            return schema
        except DatasetVersionNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {version_id} not found"
            )
    
    async def compare_version_schemas(self, version_id1: int, version_id2: int) -> Dict[str, Any]:
        """Compare schemas between two versions"""
        try:
            return await self.service.compare_version_schemas(version_id1, version_id2)
        except DatasetVersionNotFound as e:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e)
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
    
    async def attach_file_to_version(
        self,
        version_id: int,
        file: UploadFile,
        component_type: str,
        component_name: Optional[str],
        current_user: Any
    ) -> Dict[str, Any]:
        """Attach a file to an existing dataset version"""
        try:
            # Get user ID
            user_id = await self.service.get_user_id_from_soeid(current_user.soeid)
            if not user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User not found in the system"
                )
            
            # Get version to check dataset
            version = await self.service.get_dataset_version(version_id)
            if not version:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Version {version_id} not found"
                )
            
            # Check write permission
            has_permission = await self.service.check_dataset_permission(version.dataset_id, user_id, "write")
            if not has_permission:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Write permission required to attach files"
                )
            
            file_id = await self.service.attach_file_to_version(
                version_id=version_id,
                file=file,
                component_type=component_type,
                component_name=component_name,
                user_id=user_id
            )
            
            return {
                "file_id": file_id,
                "message": f"File attached successfully to version {version_id}"
            }
            
        except DatasetVersionNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {version_id} not found"
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error attaching file to version {version_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to attach file"
            )
    
    async def list_version_files(self, version_id: int) -> List[VersionFile]:
        """List all files attached to a version"""
        try:
            return await self.service.list_version_files(version_id)
        except DatasetVersionNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {version_id} not found"
            )
    
    async def get_version_file(
        self,
        version_id: int,
        component_type: str,
        component_name: Optional[str] = None
    ) -> VersionFile:
        """Get a specific file from a version"""
        try:
            version_file = await self.service.get_version_file(
                version_id=version_id,
                component_type=component_type,
                component_name=component_name
            )
            if not version_file:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"File with component type '{component_type}' not found in version {version_id}"
                )
            return version_file
        except DatasetVersionNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {version_id} not found"
            )
    
    async def create_branch(
        self,
        dataset_id: int,
        branch_name: str,
        from_version_id: int,
        current_user: Any
    ) -> DatasetPointer:
        """Create a new branch"""
        # Check write permission
        user_id = await self.service.get_user_id_from_soeid(current_user.soeid)
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not found"
            )
        
        has_permission = await self.service.check_dataset_permission(dataset_id, user_id, "write")
        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Write permission required to create branches"
            )
        
        try:
            return await self.service.create_branch(dataset_id, branch_name, from_version_id)
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
        except DatasetVersionNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {from_version_id} not found or doesn't belong to dataset"
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
    
    async def create_tag(
        self,
        dataset_id: int,
        tag_name: str,
        version_id: int,
        current_user: Any
    ) -> DatasetPointer:
        """Create a new tag"""
        # Check write permission
        user_id = await self.service.get_user_id_from_soeid(current_user.soeid)
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not found"
            )
        
        has_permission = await self.service.check_dataset_permission(dataset_id, user_id, "write")
        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Write permission required to create tags"
            )
        
        try:
            return await self.service.create_tag(dataset_id, tag_name, version_id)
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
        except DatasetVersionNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {version_id} not found or doesn't belong to dataset"
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
    
    async def update_branch(
        self,
        dataset_id: int,
        branch_name: str,
        to_version_id: int,
        current_user: Any
    ) -> Dict[str, Any]:
        """Update a branch to point to a new version"""
        # Check write permission
        user_id = await self.service.get_user_id_from_soeid(current_user.soeid)
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not found"
            )
        
        has_permission = await self.service.check_dataset_permission(dataset_id, user_id, "write")
        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Write permission required to update branches"
            )
        
        try:
            success = await self.service.update_branch(dataset_id, branch_name, to_version_id)
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to update branch"
                )
            return {
                "message": f"Branch '{branch_name}' updated to version {to_version_id}"
            }
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
        except DatasetVersionNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {to_version_id} not found or doesn't belong to dataset"
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
    
    async def list_dataset_pointers(self, dataset_id: int) -> List[DatasetPointer]:
        """List all branches and tags for a dataset"""
        try:
            return await self.service.list_dataset_pointers(dataset_id)
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
    
    async def get_pointer(self, dataset_id: int, pointer_name: str) -> DatasetPointer:
        """Get a specific pointer"""
        try:
            pointer = await self.service.get_pointer(dataset_id, pointer_name)
            if not pointer:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Pointer '{pointer_name}' not found for dataset {dataset_id}"
                )
            return pointer
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
    
    async def delete_dataset(self, dataset_id: int, current_user: Any) -> Dict[str, Any]:
        """Delete an entire dataset (requires admin permission)"""
        # Check admin permission
        user_id = await self.service.get_user_id_from_soeid(current_user.soeid)
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not found"
            )
        
        has_permission = await self.service.check_dataset_permission(dataset_id, user_id, "admin")
        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin permission required to delete dataset"
            )
        
        try:
            success = await self.service.delete_dataset(dataset_id)
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Dataset {dataset_id} not found"
                )
            return {
                "message": f"Dataset {dataset_id} deleted successfully"
            }
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
    
    async def delete_pointer(self, dataset_id: int, pointer_name: str, current_user: Any) -> Dict[str, Any]:
        """Delete a branch or tag"""
        # Check write permission
        user_id = await self.service.get_user_id_from_soeid(current_user.soeid)
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not found"
            )
        
        has_permission = await self.service.check_dataset_permission(dataset_id, user_id, "write")
        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Write permission required to delete branches/tags"
            )
        
        try:
            success = await self.service.delete_pointer(dataset_id, pointer_name)
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Pointer '{pointer_name}' not found"
                )
            return {
                "message": f"Pointer '{pointer_name}' deleted successfully"
            }
        except DatasetNotFound:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found"
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )

