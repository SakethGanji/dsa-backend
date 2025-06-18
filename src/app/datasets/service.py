"""Unified service for dataset operations"""
import os
import shutil
import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from fastapi import UploadFile
import duckdb

from app.datasets.models import (
    Dataset, DatasetCreate, DatasetUpdate, DatasetUploadRequest, DatasetUploadResponse,
    DatasetVersion, DatasetVersionCreate, File, FileCreate, Tag, SheetInfo,
    SchemaVersion, SchemaVersionCreate, VersionFile, VersionFileCreate, DatasetPointer, DatasetPointerCreate
)
from app.datasets.exceptions import DatasetNotFound, DatasetVersionNotFound, FileProcessingError, StorageError
from app.datasets.validators import DatasetValidator
from app.datasets.constants import DEFAULT_PAGE_SIZE, MAX_ROWS_PER_PAGE
from app.datasets.duckdb_service import DuckDBService

logger = logging.getLogger(__name__)


class DatasetsService:
    """Service layer for dataset operations"""
    
    def __init__(self, repository, storage_backend, user_service=None):
        self.repository = repository
        self.storage = storage_backend
        self.validator = DatasetValidator()
        self.user_service = user_service
        self._tag_cache = {}  # Simple in-memory cache
        self._cache_timestamp = None
        self._cache_ttl = 300  # 5 minutes
    
    # Dataset operations
    async def upload_dataset(
        self,
        file: UploadFile,
        request: DatasetUploadRequest,
        user_id: int,
        parent_version_id: Optional[int] = None,
        message: Optional[str] = None
    ) -> DatasetUploadResponse:
        """Process dataset upload with validation and error handling"""
        # Validate file
        file_size = file.size if hasattr(file, 'size') else 0
        self.validator.validate_file_upload(file.filename, file_size)
        
        # Validate tags
        if request.tags:
            request.tags = self.validator.validate_tags(request.tags)
        
        # Create or update dataset
        dataset_create = DatasetCreate(
            name=request.name,
            description=request.description,
            created_by=user_id,
            tags=request.tags
        )
        
        dataset_id = await self.repository.upsert_dataset(request.dataset_id, dataset_create)
        
        # Grant admin permission to creator for new datasets
        if not request.dataset_id and self.user_service:
            from app.users.models import DatasetPermissionType
            await self.user_service.grant_dataset_permission(
                dataset_id,
                user_id,
                DatasetPermissionType.ADMIN
            )
        
        # Handle branch context
        branch_name = request.branch_name or "main"
        
        # For existing datasets, determine parent version from branch
        if request.dataset_id and not parent_version_id:
            # Get the current version of the target branch
            branch_pointer = await self.repository.get_pointer(dataset_id, branch_name)
            if branch_pointer:
                parent_version_id = branch_pointer.dataset_version_id
            elif branch_name != "main":
                # Branch doesn't exist
                raise ValueError(f"Branch '{branch_name}' does not exist for dataset {dataset_id}")
        
        # Get the original file type
        file_type = os.path.splitext(file.filename)[1].lower()[1:]
        
        # Save file to storage
        file_id, file_path = await self._save_file(file, dataset_id, file_type)
        
        # Create dataset version with parent support
        version_id = await self._create_dataset_version(
            dataset_id=dataset_id,
            file_id=file_id,
            user_id=user_id,
            parent_version_id=parent_version_id,
            message=message
        )
        
        # Create version-file association
        await self._attach_file_to_version(
            version_id=version_id,
            file_id=file_id,
            component_type="primary",
            component_name="main"
        )
        
        # Process tags
        if request.tags:
            await self._process_tags(dataset_id, request.tags)
        
        # Update file path with version ID
        if file_path:
            file_path = await self._finalize_file_location(file_id, file_path, version_id)
        
        # Parse file into sheets
        try:
            sheet_infos = await self._parse_file_into_sheets(file_path, file.filename, version_id)
        except Exception as e:
            logger.error(f"Error parsing file into sheets: {str(e)}")
            # Return empty sheet info for now
            sheet_infos = []
        
        # Extract and store schema
        await self._capture_schema(file_path, file_type, version_id)
        
        # Update branch pointer to new version
        branch_pointer = await self.repository.get_pointer(dataset_id, branch_name)
        if branch_pointer:
            await self.repository.update_pointer(dataset_id, branch_name, version_id)
        else:
            # Create branch if it doesn't exist (should only happen for "main" on first upload)
            await self.create_branch(dataset_id, branch_name, version_id)
        
        return DatasetUploadResponse(
            dataset_id=dataset_id,
            version_id=version_id,
            sheets=sheet_infos
        )
    
    async def list_datasets(self, **kwargs) -> List[Dataset]:
        """List datasets with filtering and pagination"""
        # Validate and normalize inputs
        limit = kwargs.get('limit', DEFAULT_PAGE_SIZE)
        if limit < 1:
            limit = 10
        elif limit > 100:
            limit = 100
        
        offset = kwargs.get('offset', 0)
        if offset < 0:
            offset = 0
        
        kwargs['limit'] = limit
        kwargs['offset'] = offset
        
        return await self.repository.list_datasets(**kwargs)
    
    async def get_dataset(self, dataset_id: int) -> Optional[Dataset]:
        """Get detailed information about a single dataset"""
        result = await self.repository.get_dataset(dataset_id)
        if not result:
            raise DatasetNotFound(dataset_id)
        return result
    
    async def update_dataset(self, dataset_id: int, data: DatasetUpdate) -> Optional[Dataset]:
        """Update dataset metadata including name, description, and tags"""
        # First check if dataset exists
        existing_dataset = await self.get_dataset(dataset_id)
        if not existing_dataset:
            raise DatasetNotFound(dataset_id)
        
        # Update basic metadata
        updated_id = await self.repository.update_dataset(dataset_id, data)
        if not updated_id:
            return None
        
        # If tags were provided, update tags
        if data.tags is not None:
            # Delete all existing tags
            await self.repository.delete_dataset_tags(dataset_id)
            
            # Add new tags
            for tag_name in data.tags:
                tag_id = await self.repository.upsert_tag(tag_name)
                await self.repository.create_dataset_tag(dataset_id, tag_id)
        
        # Return the updated dataset
        return await self.get_dataset(dataset_id)
    
    # Version operations
    async def list_dataset_versions(self, dataset_id: int, branch_name: Optional[str] = None) -> List[DatasetVersion]:
        """List all versions of a dataset, optionally filtered by branch"""
        # Check if dataset exists
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        if branch_name:
            # Get versions on specific branch
            return await self.get_branch_history(dataset_id, branch_name)
        else:
            # Get all versions
            return await self.repository.list_dataset_versions(dataset_id)
    
    async def get_version_tree(self, dataset_id: int) -> Dict[str, Any]:
        """Get version tree/DAG structure for a dataset"""
        # Check if dataset exists
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        versions = await self.repository.list_dataset_versions(dataset_id)
        
        # Build tree structure
        version_dict = {v.id: v for v in versions}
        roots = []
        
        for version in versions:
            if not version.parent_version_id:
                roots.append(version)
        
        def build_tree(version):
            children = [v for v in versions if v.parent_version_id == version.id]
            return {
                "id": version.id,
                "version_number": version.version_number,
                "message": version.message,
                "created_at": version.ingestion_timestamp.isoformat(),
                "children": [build_tree(child) for child in children]
            }
        
        return {
            "dataset_id": dataset_id,
            "roots": [build_tree(root) for root in roots]
        }
    
    async def get_dataset_version(self, version_id: int) -> Optional[DatasetVersion]:
        """Get detailed information about a single dataset version"""
        result = await self.repository.get_dataset_version(version_id)
        if not result:
            raise DatasetVersionNotFound(version_id)
        return result
    
    async def get_version_from_branch(
        self, 
        dataset_id: int, 
        branch_name: str, 
        version_ref: Optional[str] = "HEAD"
    ) -> Optional[DatasetVersion]:
        """Get a specific version from a branch
        
        Args:
            dataset_id: The dataset ID
            branch_name: The branch name
            version_ref: Version reference - "HEAD" for latest, or version number
        """
        if version_ref == "HEAD":
            # Get the latest version on the branch
            return await self.get_branch_head(dataset_id, branch_name)
        else:
            # Get specific version number on the branch
            branch_history = await self.get_branch_history(dataset_id, branch_name)
            
            try:
                target_version_num = int(version_ref)
                for version in branch_history:
                    if version.version_number == target_version_num:
                        return version
            except ValueError:
                raise ValueError(f"Invalid version reference: {version_ref}")
            
            return None
    
    async def get_dataset_version_file(self, version_id: int) -> Optional[File]:
        """Get primary file information for a dataset version"""
        version = await self.get_dataset_version(version_id)
        if not version:
            return None
        
        # Try to get primary file from version_files table first
        primary_file = await self.repository.get_version_file_by_component(
            version_id, "primary", "main"
        )
        
        if primary_file and primary_file.file:
            return primary_file.file
        
        # Fallback to legacy file_id if no primary file in version_files
        if version.file_id:
            return await self.repository.get_file(version.file_id)
        
        return None
    
    async def delete_dataset(self, dataset_id: int) -> bool:
        """Delete an entire dataset and all its versions"""
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        # Delete all versions first
        versions = await self.repository.list_dataset_versions(dataset_id)
        for version in versions:
            await self.delete_dataset_version(version.id)
        
        # Delete all pointers
        pointers = await self.repository.list_dataset_pointers(dataset_id)
        for pointer in pointers:
            await self.repository.delete_pointer(dataset_id, pointer.pointer_name)
        
        # Delete all tags associations
        await self.repository.delete_dataset_tags(dataset_id)
        
        # Delete all permissions
        if self.user_service:
            # Get all permissions for this dataset
            permissions = await self.user_service.list_dataset_permissions(dataset_id)
            # Revoke each permission
            for perm in permissions:
                await self.user_service.revoke_dataset_permission(
                    dataset_id,
                    perm.user_id,
                    perm.permission_type
                )
        
        # Finally delete the dataset itself
        deleted_id = await self.repository.delete_dataset(dataset_id)
        return deleted_id is not None
    
    async def delete_dataset_version(self, version_id: int) -> bool:
        """Delete a dataset version"""
        version = await self.get_dataset_version(version_id)
        if not version:
            raise DatasetVersionNotFound(version_id)
        
        # Get all files attached to this version
        version_files = await self.repository.list_version_files(version_id)
        
        # Delete version-file associations first
        await self.repository.delete_version_files(version_id)
        
        # Decrement reference counts for all files
        for vf in version_files:
            await self._decrement_file_reference_count(vf.file_id)
        
        # Delete the version itself
        deleted_id = await self.repository.delete_dataset_version(version_id)
        return deleted_id is not None
    
    # Tag operations with simple caching
    async def list_tags(self) -> List[Tag]:
        """List all available tags with simple caching"""
        from datetime import datetime
        
        # Check cache
        if self._cache_timestamp:
            age = (datetime.now() - self._cache_timestamp).total_seconds()
            if age < self._cache_ttl and 'tags' in self._tag_cache:
                return self._tag_cache['tags']
        
        # Fetch from database
        tags = await self.repository.list_tags()
        
        # Update cache
        self._tag_cache['tags'] = tags
        self._cache_timestamp = datetime.now()
        
        return tags
    
    # Sheet operations
    async def list_version_sheets(self, version_id: int) -> List[SheetInfo]:
        """Get all sheets for a dataset version"""
        version = await self.get_dataset_version(version_id)
        if not version:
            return []
        
        return await self.repository.list_version_sheets(version_id)
    
    async def get_sheet_data(
        self,
        version_id: int,
        sheet_name: Optional[str],
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[str], List[Dict[str, Any]], bool]:
        """Get paginated data from a sheet"""
        # First check if version exists
        version = await self.get_dataset_version(version_id)
        if not version:
            return [], [], False
        
        # Get file info
        file_info = await self.repository.get_file(version.file_id)
        if not file_info:
            logger.error(f"File not found for version {version_id}, file_id: {version.file_id}")
            return [], [], False
        
        # Get sheets to validate sheet_name
        sheets = await self.repository.list_version_sheets(version_id)
        sheet_names = [sheet.name for sheet in sheets]
        
        # For single sheet files, use the first sheet
        if not sheet_name and sheets:
            sheet_name = sheets[0].name
        
        # Validate sheet name
        if sheet_name and sheet_name not in sheet_names:
            logger.error(f"Sheet '{sheet_name}' not found in available sheets: {sheet_names}")
            return [], [], False
        
        # Read data from Parquet file using DuckDB
        try:
            if file_info.file_path:
                return await self._read_parquet_data(file_info.file_path, limit, offset)
            else:
                logger.error(f"File path not found for file {file_info.id}")
                return [], [], False
        except Exception as e:
            logger.error(f"Error reading file data: {str(e)}")
            return [], [], False
    
    # User operations
    async def get_user_id_from_soeid(self, soeid: str) -> Optional[int]:
        """Get user ID from soeid"""
        from app.users.repository import get_user_by_soeid
        user = await get_user_by_soeid(self.repository.session, soeid)
        return user["id"] if user else None
    
    # Private helper methods
    async def _save_file(self, file: UploadFile, dataset_id: int, file_type: str) -> Tuple[int, str]:
        """Save file to storage and create database record"""
        try:
            # Read file content
            file_content = await file.read()
            await file.seek(0)
            
            # Save file as Parquet using storage backend
            result = await self.storage.save_dataset_file(
                file_content=file_content,
                dataset_id=dataset_id,
                version_id=0,  # Temporary, will update later
                file_name=file.filename
            )
            
            file_path = result["path"]
            file_size = result["size"]
            
            # Create file record in database
            file_create = FileCreate(
                storage_type="filesystem",
                file_type="parquet",
                mime_type="application/parquet",
                file_data=None,
                file_size=file_size,
                file_path=file_path
            )
            file_id = await self.repository.create_file(file_create)
            logger.info(f"File saved to storage: {file_path}, size: {file_size} bytes")
            
            return file_id, file_path
            
        except Exception as e:
            logger.error(f"Error saving file: {str(e)}")
            raise StorageError("save_file", str(e))
    
    async def _create_dataset_version(
        self,
        dataset_id: int,
        file_id: int,
        user_id: int,
        parent_version_id: Optional[int] = None,
        message: Optional[str] = None,
        overlay_file_id: Optional[int] = None
    ) -> int:
        """Create a new dataset version with optional parent reference"""
        # Determine version number based on parent
        if parent_version_id:
            parent_version = await self.repository.get_dataset_version(parent_version_id)
            if parent_version:
                # For branching, version number continues from parent
                version_number = parent_version.version_number + 1
            else:
                raise DatasetVersionNotFound(parent_version_id)
        else:
            # No parent, get next version number in sequence
            version_number = await self.repository.get_next_version_number(dataset_id)
        
        version_create = DatasetVersionCreate(
            dataset_id=dataset_id,
            version_number=version_number,
            file_id=file_id,
            uploaded_by=user_id,
            parent_version_id=parent_version_id,
            message=message,
            overlay_file_id=overlay_file_id
        )
        
        version_id = await self.repository.create_dataset_version(version_create)
        await self.repository.update_dataset_timestamp(dataset_id)
        
        return version_id
    
    async def _process_tags(self, dataset_id: int, tags: List[str]) -> None:
        """Process and associate tags with dataset"""
        for tag_name in tags:
            tag_id = await self.repository.upsert_tag(tag_name)
            await self.repository.create_dataset_tag(dataset_id, tag_id)
    
    async def _finalize_file_location(
        self,
        file_id: int,
        file_path: str,
        version_id: int
    ) -> str:
        """Move file to final location with version ID"""
        new_path = file_path.replace("/temp/", f"/{version_id}/").replace("_temp_", f"_{version_id}_")
        os.makedirs(os.path.dirname(new_path), exist_ok=True)
        shutil.move(file_path, new_path)
        
        await self.repository.update_file_path(file_id, new_path)
        return new_path
    
    async def _parse_file_into_sheets(
        self,
        file_path: str,
        original_filename: str,
        version_id: int
    ) -> List[SheetInfo]:
        """Parse file and create sheet metadata"""
        sheet_infos = []
        
        # For now, just return basic sheet info without creating entries in dataset_version_files
        # This avoids transaction issues while we transition to the new schema
        try:
            # Use DuckDB to read Parquet metadata
            conn = duckdb.connect(':memory:')
            try:
                # Create view from Parquet file
                conn.execute(f"CREATE VIEW parquet_data AS SELECT * FROM read_parquet('{file_path}')")
                
                # Get metadata
                columns_info = conn.execute("PRAGMA table_info('parquet_data')").fetchall()
                column_names = [col[1] for col in columns_info]
                
                # Create sheet entry
                sheet_name = os.path.splitext(original_filename)[0]
                
                # For now, just return the sheet info without persisting
                sheet_infos.append(SheetInfo(
                    file_id=None,  # We'll set this later when we have a stable transaction
                    name=sheet_name,
                    index=0,
                    description=None
                ))
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Error parsing Parquet file: {str(e)}")
            # Return basic error sheet info
            filename = os.path.basename(original_filename)
            sheet_infos.append(SheetInfo(
                file_id=None,
                name=filename,
                index=0,
                description="Error parsing file"
            ))
            
        return sheet_infos
    
    async def _create_error_sheet(
        self,
        file_path: str,
        version_id: int,
        file_type: str,
        error_msg: str,
        original_filename: str
    ) -> List[SheetInfo]:
        """Create a sheet entry for a file parsing error"""
        filename = os.path.basename(original_filename)
        
        # For now, just return error sheet info without persisting
        return [SheetInfo(
            file_id=None,
            name=filename,
            index=0,
            description="Error parsing file"
        )]
    
    async def _read_parquet_data(
        self,
        file_path: str,
        limit: int,
        offset: int
    ) -> Tuple[List[str], List[Dict[str, Any]], bool]:
        """Read data from Parquet file using DuckDB"""
        conn = duckdb.connect(':memory:')
        try:
            # Create view from Parquet file
            conn.execute(f"CREATE VIEW sheet_data AS SELECT * FROM read_parquet('{file_path}')")
            
            # Get headers
            columns_info = conn.execute("PRAGMA table_info('sheet_data')").fetchall()
            headers = [col[1] for col in columns_info]
            
            # Get total row count
            total_rows = conn.execute("SELECT COUNT(*) FROM sheet_data").fetchone()[0]
            
            # Get paginated data
            result = conn.execute(f"SELECT * FROM sheet_data LIMIT {limit} OFFSET {offset}").fetchall()
            
            # Convert to list of dicts
            rows = []
            for row in result:
                row_dict = {}
                for i, value in enumerate(row):
                    row_dict[headers[i]] = value
                rows.append(row_dict)
            
            # Check if there's more data
            has_more = (offset + limit) < total_rows
            
            return headers, rows, has_more
        finally:
            conn.close()
    
    async def _capture_schema(self, file_path: str, file_type: str, version_id: int) -> None:
        """Extract and store schema for a dataset version"""
        try:
            # Extract schema using DuckDB service
            schema_json = await DuckDBService.extract_schema(file_path, file_type)
            
            # Check if extraction was successful
            if "error" in schema_json:
                logger.error(f"Schema extraction failed for version {version_id}: {schema_json['error']}")
                return
            
            # Create schema version
            schema_create = SchemaVersionCreate(
                dataset_version_id=version_id,
                schema_data=schema_json
            )
            
            await self.repository.create_schema_version(schema_create)
            logger.info(f"Schema captured for version {version_id}")
            
        except Exception as e:
            logger.error(f"Error capturing schema for version {version_id}: {str(e)}")
            # Don't fail the upload if schema capture fails
    
    async def get_schema_for_version(self, version_id: int) -> Optional[SchemaVersion]:
        """Get schema information for a dataset version"""
        version = await self.get_dataset_version(version_id)
        if not version:
            raise DatasetVersionNotFound(version_id)
        
        return await self.repository.get_schema_version(version_id)
    
    async def compare_version_schemas(self, version_id1: int, version_id2: int) -> Dict[str, Any]:
        """Compare schemas between two dataset versions"""
        # Verify both versions exist
        version1 = await self.get_dataset_version(version_id1)
        version2 = await self.get_dataset_version(version_id2)
        
        if not version1:
            raise DatasetVersionNotFound(version_id1)
        if not version2:
            raise DatasetVersionNotFound(version_id2)
        
        # Ensure both versions belong to the same dataset
        if version1.dataset_id != version2.dataset_id:
            raise ValueError("Versions belong to different datasets")
        
        return await self.repository.compare_schemas(version_id1, version_id2)
    
    async def _attach_file_to_version(
        self,
        version_id: int,
        file_id: int,
        component_type: str,
        component_name: Optional[str] = None,
        component_index: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Attach a file to a dataset version"""
        version_file = VersionFileCreate(
            version_id=version_id,
            file_id=file_id,
            component_type=component_type,
            component_name=component_name,
            component_index=component_index,
            metadata=metadata
        )
        await self.repository.create_version_file(version_file)
    
    async def attach_file_to_version(
        self,
        version_id: int,
        file: UploadFile,
        component_type: str,
        component_name: Optional[str] = None,
        user_id: int = None
    ) -> int:
        """Attach an additional file to an existing dataset version"""
        # Verify version exists
        version = await self.get_dataset_version(version_id)
        if not version:
            raise DatasetVersionNotFound(version_id)
        
        # Save the file
        file_type = os.path.splitext(file.filename)[1].lower()[1:]
        file_id, file_path = await self._save_file(file, version.dataset_id, file_type)
        
        # Update file path with version ID
        if file_path:
            file_path = await self._finalize_file_location(file_id, file_path, version_id)
        
        # Attach to version
        await self._attach_file_to_version(
            version_id=version_id,
            file_id=file_id,
            component_type=component_type,
            component_name=component_name or file.filename,
            metadata={"original_filename": file.filename}
        )
        
        # Update reference count for the file
        await self._increment_file_reference_count(file_id)
        
        return file_id
    
    async def list_version_files(self, version_id: int) -> List[VersionFile]:
        """List all files attached to a dataset version"""
        version = await self.get_dataset_version(version_id)
        if not version:
            raise DatasetVersionNotFound(version_id)
        
        return await self.repository.list_version_files(version_id)
    
    async def get_version_file(
        self, 
        version_id: int, 
        component_type: str,
        component_name: Optional[str] = None
    ) -> Optional[VersionFile]:
        """Get a specific file from a version by component"""
        version = await self.get_dataset_version(version_id)
        if not version:
            raise DatasetVersionNotFound(version_id)
        
        return await self.repository.get_version_file_by_component(
            version_id, component_type, component_name
        )
    
    async def _increment_file_reference_count(self, file_id: int) -> None:
        """Increment reference count for a file (for Phase 1 compatibility)"""
        # This would be implemented if Phase 1 reference counting is in place
        # For now, we'll log it
        logger.info(f"Reference count increment needed for file {file_id}")
    
    async def _decrement_file_reference_count(self, file_id: int) -> None:
        """Decrement reference count for a file (for Phase 1 compatibility)"""
        # This would be implemented if Phase 1 reference counting is in place
        # For now, we'll log it
        logger.info(f"Reference count decrement needed for file {file_id}")
    
    # Branch and tag operations
    async def create_branch(
        self,
        dataset_id: int,
        branch_name: str,
        from_version_id: int
    ) -> DatasetPointer:
        """Create a new branch pointing to a specific version"""
        # Validate dataset exists
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        # Validate version exists and belongs to dataset
        version = await self.get_dataset_version(from_version_id)
        if not version or version.dataset_id != dataset_id:
            raise DatasetVersionNotFound(from_version_id)
        
        # Validate branch name
        if not branch_name or len(branch_name) > 255:
            raise ValueError("Invalid branch name")
        
        # Create branch pointer
        pointer = DatasetPointerCreate(
            dataset_id=dataset_id,
            pointer_name=branch_name,
            dataset_version_id=from_version_id,
            is_tag=False
        )
        
        pointer_id = await self.repository.create_pointer(pointer)
        return await self.repository.get_pointer(dataset_id, branch_name)
    
    async def create_tag(
        self,
        dataset_id: int,
        tag_name: str,
        version_id: int
    ) -> DatasetPointer:
        """Create an immutable tag pointing to a specific version"""
        # Validate dataset exists
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        # Validate version exists and belongs to dataset
        version = await self.get_dataset_version(version_id)
        if not version or version.dataset_id != dataset_id:
            raise DatasetVersionNotFound(version_id)
        
        # Validate tag name
        if not tag_name or len(tag_name) > 255:
            raise ValueError("Invalid tag name")
        
        # Check if tag already exists (tags are immutable)
        existing = await self.repository.get_pointer(dataset_id, tag_name)
        if existing and existing.is_tag:
            raise ValueError(f"Tag '{tag_name}' already exists")
        
        # Create tag pointer
        pointer = DatasetPointerCreate(
            dataset_id=dataset_id,
            pointer_name=tag_name,
            dataset_version_id=version_id,
            is_tag=True
        )
        
        pointer_id = await self.repository.create_pointer(pointer)
        return await self.repository.get_pointer(dataset_id, tag_name)
    
    async def commit_to_branch(
        self,
        dataset_id: int,
        branch_name: str,
        file: UploadFile,
        user_id: int,
        message: Optional[str] = None
    ) -> DatasetUploadResponse:
        """Create a new version on a specific branch"""
        # Validate dataset exists
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        # Get the branch pointer
        branch_pointer = await self.repository.get_pointer(dataset_id, branch_name)
        if not branch_pointer:
            raise ValueError(f"Branch '{branch_name}' does not exist")
        if branch_pointer.is_tag:
            raise ValueError(f"Cannot commit to tag '{branch_name}' - tags are immutable")
        
        # Create upload request with branch context
        upload_request = DatasetUploadRequest(
            dataset_id=dataset_id,
            name=dataset.name,
            description=dataset.description,
            branch_name=branch_name
        )
        
        # Use the existing upload_dataset method with branch context
        return await self.upload_dataset(
            file=file,
            request=upload_request,
            user_id=user_id,
            parent_version_id=branch_pointer.dataset_version_id,
            message=message
        )
    
    async def update_branch(
        self,
        dataset_id: int,
        branch_name: str,
        to_version_id: int
    ) -> bool:
        """Update a branch to point to a new version"""
        # Validate dataset exists
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        # Validate version exists and belongs to dataset
        version = await self.get_dataset_version(to_version_id)
        if not version or version.dataset_id != dataset_id:
            raise DatasetVersionNotFound(to_version_id)
        
        # Check if pointer exists and is a branch
        pointer = await self.repository.get_pointer(dataset_id, branch_name)
        if not pointer:
            raise ValueError(f"Branch '{branch_name}' not found")
        if pointer.is_tag:
            raise ValueError(f"Cannot update tag '{branch_name}' - tags are immutable")
        
        return await self.repository.update_pointer(dataset_id, branch_name, to_version_id)
    
    async def list_dataset_pointers(self, dataset_id: int) -> List[DatasetPointer]:
        """List all branches and tags for a dataset"""
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        return await self.repository.list_dataset_pointers(dataset_id)
    
    async def get_pointer(self, dataset_id: int, pointer_name: str) -> Optional[DatasetPointer]:
        """Get a specific pointer (branch or tag)"""
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        return await self.repository.get_pointer(dataset_id, pointer_name)
    
    async def delete_pointer(self, dataset_id: int, pointer_name: str) -> bool:
        """Delete a branch or tag"""
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        # Don't allow deleting 'main' branch
        if pointer_name.lower() == 'main':
            raise ValueError("Cannot delete 'main' branch")
        
        return await self.repository.delete_pointer(dataset_id, pointer_name)
    
    async def resolve_pointer_to_version(self, dataset_id: int, ref: str) -> Optional[int]:
        """Resolve a reference (pointer name or version number) to a version ID"""
        # Try to parse as version number first
        try:
            version_num = int(ref)
            # Find version by number
            versions = await self.list_dataset_versions(dataset_id)
            for v in versions:
                if v.version_number == version_num:
                    return v.id
            return None
        except ValueError:
            # Not a number, try as pointer name
            return await self.repository.resolve_pointer_to_version(dataset_id, ref)
    
    # Branch navigation methods
    async def get_branch_head(self, dataset_id: int, branch_name: str) -> Optional[DatasetVersion]:
        """Get the latest version on a branch"""
        # Validate dataset exists
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        # Get branch pointer
        pointer = await self.repository.get_pointer(dataset_id, branch_name)
        if not pointer:
            return None
        
        # Get the version the branch points to
        return await self.get_dataset_version(pointer.dataset_version_id)
    
    async def list_branches(self, dataset_id: int) -> List[DatasetPointer]:
        """List all branches (not tags) for a dataset"""
        pointers = await self.list_dataset_pointers(dataset_id)
        return [p for p in pointers if not p.is_tag]
    
    async def list_tags(self, dataset_id: int) -> List[DatasetPointer]:
        """List all tags (not branches) for a dataset"""
        pointers = await self.list_dataset_pointers(dataset_id)
        return [p for p in pointers if p.is_tag]
    
    async def get_branch_history(self, dataset_id: int, branch_name: str) -> List[DatasetVersion]:
        """Get the commit history for a branch by following parent links"""
        # Get branch head
        branch_head = await self.get_branch_head(dataset_id, branch_name)
        if not branch_head:
            return []
        
        history = []
        current_version = branch_head
        
        # Follow parent links
        while current_version:
            history.append(current_version)
            if current_version.parent_version_id:
                current_version = await self.get_dataset_version(current_version.parent_version_id)
            else:
                break
        
        return history
    
    async def branch_exists(self, dataset_id: int, branch_name: str) -> bool:
        """Check if a branch exists"""
        pointer = await self.repository.get_pointer(dataset_id, branch_name)
        return pointer is not None and not pointer.is_tag
    
    # Permission helpers
    async def check_dataset_permission(
        self,
        dataset_id: int,
        user_id: int,
        permission_type: str
    ) -> bool:
        """Check if user has permission for a dataset"""
        if not self.user_service:
            # No permission service configured, allow all
            return True
        
        from app.users.models import DatasetPermissionType
        
        # Convert string to enum
        perm_type = DatasetPermissionType(permission_type)
        
        return await self.user_service.check_dataset_permission(
            dataset_id,
            user_id,
            perm_type
        )