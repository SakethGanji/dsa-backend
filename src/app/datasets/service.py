"""Unified service for dataset operations"""
import os
import shutil
import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from fastapi import UploadFile
import duckdb
from sqlalchemy import text

from app.core.events import Event, EventType

from app.datasets.models import (
    Dataset, DatasetCreate, DatasetUpdate, DatasetUploadRequest, DatasetUploadResponse,
    DatasetVersion, DatasetVersionCreate, File, FileCreate, Tag, SheetInfo,
    SchemaVersion, SchemaVersionCreate, VersionFile, VersionFileCreate,
    VersionTag, VersionTagCreate, OverlayData, OverlayFileAction, FileOperation,
    VersionResolution, VersionResolutionType,
    VersionCreateRequest, VersionCreateResponse
)
from app.datasets.exceptions import DatasetNotFound, DatasetVersionNotFound, FileProcessingError, StorageError
from app.datasets.validators import DatasetValidator
from app.datasets.constants import DEFAULT_PAGE_SIZE, MAX_ROWS_PER_PAGE
from app.datasets.duckdb_service import DuckDBService
from app.datasets.statistics_service import StatisticsService

logger = logging.getLogger(__name__)


class DatasetsService:
    """Service layer for dataset operations"""
    
    def __init__(self, repository, storage_backend, user_service=None, artifact_producer=None, event_bus=None):
        self.repository = repository
        self.storage = storage_backend
        self.validator = DatasetValidator()
        self.user_service = user_service
        self.artifact_producer = artifact_producer
        self.event_bus = event_bus
        self._tag_cache = {}  # Simple in-memory cache
        self._cache_timestamp = None
        self._cache_ttl = 300  # 5 minutes
    
    # Dataset operations
    async def upload_dataset(
        self,
        file: UploadFile,
        request: DatasetUploadRequest,
        user_id: int
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
        
        # If no dataset_id provided, check if dataset with same name exists for this user
        existing_dataset_id = None
        if request.dataset_id is None:
            existing_dataset_id = await self.repository.get_dataset_by_name_and_user(
                request.name, user_id
            )
            if existing_dataset_id:
                dataset_id = existing_dataset_id
            else:
                dataset_id = await self.repository.upsert_dataset(None, dataset_create)
        else:
            dataset_id = await self.repository.upsert_dataset(request.dataset_id, dataset_create)
        
        # Grant admin permission to creator for new datasets
        if not request.dataset_id and not existing_dataset_id and self.user_service:
            from app.users.models import DatasetPermissionType
            await self.user_service.grant_dataset_permission(
                dataset_id,
                user_id,
                DatasetPermissionType.ADMIN
            )
            
        # Publish dataset created event for new datasets
        if not request.dataset_id and not existing_dataset_id and self.event_bus:
            await self.event_bus.publish(Event(
                event_type=EventType.DATASET_CREATED,
                timestamp=datetime.utcnow(),
                data={
                    "dataset_id": dataset_id,
                    "dataset_name": request.name,
                    "created_by": user_id,
                    "tags": request.tags or []
                },
                source="DatasetsService"
            ))
        
        
        # Get the original file type
        file_type = os.path.splitext(file.filename)[1].lower()[1:]
        
        # Save file to storage
        file_id, file_path, used_artifact_producer = await self._save_file(file, dataset_id, file_type)
        
        # Create dataset version using overlay-based approach for consistency
        file_changes = [OverlayFileAction(
            operation=FileOperation.ADD,
            file_id=file_id,
            component_name="main",
            component_type="primary"
        )]
        
        version_request = VersionCreateRequest(
            dataset_id=dataset_id,
            file_changes=file_changes,
            message=f"Uploaded {file.filename}",
            parent_version=None
        )
        
        version_response = await self.create_version_from_changes(version_request, user_id)
        version_id = version_response.version_id
        
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
            
        # Publish file uploaded event
        if self.event_bus:
            await self.event_bus.publish(Event(
                event_type=EventType.FILE_UPLOADED,
                timestamp=datetime.utcnow(),
                data={
                    "file_id": file_id,
                    "dataset_id": dataset_id,
                    "file_path": file_path,
                    "file_type": file_type,
                    "file_size": file_size if hasattr(file, 'size') else 0
                },
                source="DatasetsService"
            ))
            
        # Publish version created event
        if self.event_bus:
            await self.event_bus.publish(Event(
                event_type=EventType.VERSION_CREATED,
                timestamp=datetime.utcnow(),
                data={
                    "dataset_id": dataset_id,
                    "version_id": version_id,
                    "version_number": version_response.version_number,
                    "parent_version_id": None
                },
                source="DatasetsService"
            ))
        
        # Update file path with version ID only if artifact producer was NOT used
        if file_path and not used_artifact_producer:
            file_path = await self._finalize_file_location(file_id, file_path, version_id)
        elif file_path and used_artifact_producer:
            # When using artifact producer, the file_path might be a URI
            if file_path.startswith("file://"):
                # Extract the actual path from the URI
                from urllib.parse import urlparse
                parsed = urlparse(file_path)
                file_path = parsed.path
            elif not os.path.isabs(file_path):
                # If it's a relative path, make it absolute
                base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
                file_path = os.path.join(base_path, file_path)
        
        # Parse file into sheets
        try:
            sheet_infos = await self._parse_file_into_sheets(file_path, file.filename, version_id)
        except Exception as e:
            logger.error(f"Error parsing file into sheets: {str(e)}")
            # Return empty sheet info for now
            sheet_infos = []
        
        # Extract and store schema
        await self._capture_schema(file_path, file_type, version_id)
        
        # Calculate and store statistics
        try:
            logger.info(f"Calculating statistics for version {version_id}")
            stats = await StatisticsService.calculate_parquet_statistics(file_path, detailed=False)
            
            # Store in dataset_statistics table
            await self.repository.upsert_dataset_statistics(
                version_id=version_id,
                row_count=stats["row_count"],
                column_count=stats["column_count"],
                size_bytes=stats["size_bytes"],
                statistics=stats["statistics"]
            )
            
            # Create analysis_run record for tracking
            await self.repository.create_analysis_run(
                dataset_version_id=version_id,
                user_id=user_id,
                run_type="profiling",
                run_parameters={"method": "parquet_metadata", "detailed": False},
                status="completed",
                output_summary=stats["statistics"]
            )
            
            logger.info(f"Statistics calculated and stored for version {version_id}")
        except Exception as e:
            logger.error(f"Error calculating statistics: {str(e)}")
            # Don't fail the upload if statistics calculation fails
        
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
        
        # Publish dataset updated event
        if self.event_bus:
            # Determine what changed
            changes = {}
            if data.name is not None:
                changes["name"] = data.name
            if data.description is not None:
                changes["description"] = data.description
            if data.tags is not None:
                changes["tags"] = data.tags
                
            await self.event_bus.publish(Event(
                event_type=EventType.DATASET_UPDATED,
                timestamp=datetime.utcnow(),
                data={
                    "dataset_id": dataset_id,
                    "updated_fields": changes,
                    "previous_values": {
                        "name": existing_dataset.name,
                        "description": existing_dataset.description,
                        "tags": [tag.tag_name for tag in existing_dataset.tags] if existing_dataset.tags else []
                    }
                },
                source="DatasetsService"
            ))
        
        # Return the updated dataset
        return await self.get_dataset(dataset_id)
    
    # Version operations
    async def list_dataset_versions(self, dataset_id: int) -> List[DatasetVersion]:
        """List all versions of a dataset"""
        # Check if dataset exists
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        # Get all versions
        return await self.repository.list_dataset_versions(dataset_id)
    
    
    async def get_dataset_version(self, version_id: int) -> Optional[DatasetVersion]:
        """Get detailed information about a single dataset version"""
        result = await self.repository.get_dataset_version(version_id)
        if not result:
            raise DatasetVersionNotFound(version_id)
        return result
    
    
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
        
        # Use overlay file if no primary file in version_files
        file_id = version.overlay_file_id
        if file_id:
            return await self.repository.get_file(file_id)
        
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
        
        # Get file IDs before deletion for event
        file_ids = []
        for version in versions:
            version_files = await self.repository.list_version_files(version.id)
            file_ids.extend([vf.file_id for vf in version_files])
        
        # Finally delete the dataset itself
        deleted_id = await self.repository.delete_dataset(dataset_id)
        
        # Publish dataset deleted event
        if deleted_id and self.event_bus:
            await self.event_bus.publish(Event(
                event_type=EventType.DATASET_DELETED,
                timestamp=datetime.utcnow(),
                data={
                    "dataset_id": dataset_id,
                    "file_ids": file_ids
                },
                source="DatasetsService"
            ))
        
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
        
        # Note: File reference counting removed as part of EDA simplification
        
        # Delete the version itself
        deleted_id = await self.repository.delete_dataset_version(version_id)
        
        # Publish version deleted event
        if deleted_id and self.event_bus:
            await self.event_bus.publish(Event(
                event_type=EventType.VERSION_DELETED,
                timestamp=datetime.utcnow(),
                data={
                    "dataset_id": version.dataset_id,
                    "version_id": version_id,
                    "version_number": version.version_number,
                    "file_ids": [vf.file_id for vf in version_files]
                },
                source="DatasetsService"
            ))
        
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
        
        # Get primary file info from version_files table
        primary_file = await self.repository.get_version_file_by_component(
            version_id, "primary", "main"
        )
        
        if primary_file and primary_file.file:
            file_info = primary_file.file
        else:
            # Fallback to overlay file if no primary file in version_files
            file_id = version.overlay_file_id
            file_info = await self.repository.get_file(file_id)
            
        if not file_info:
            logger.error(f"File not found for version {version_id}")
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
                # Handle different path formats
                file_path = file_info.file_path
                if file_path.startswith("file://"):
                    # Extract the actual path from the URI
                    from urllib.parse import urlparse
                    parsed = urlparse(file_path)
                    file_path = parsed.path
                elif not os.path.isabs(file_path):
                    # Construct absolute path for relative paths
                    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
                    file_path = os.path.join(base_path, file_path)
                
                logger.info(f"Reading data from file: {file_path}")
                return await self._read_parquet_data(file_path, limit, offset)
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
            # If artifact_producer is available, use it for centralized file creation
            if self.artifact_producer:
                import io
                
                # Read file content for conversion
                file_content = await file.read()
                await file.seek(0)
                
                # First convert to Parquet if needed (using existing logic)
                result = await self.storage.save_dataset_file(
                    file_content=file_content,
                    dataset_id=dataset_id,
                    version_id=0,  # Temporary, will update later
                    file_name=file.filename
                )
                
                # Now read the converted Parquet file and use artifact producer
                with open(result["path"], 'rb') as parquet_file:
                    file_id = await self.artifact_producer.create_artifact(
                        content_stream=parquet_file,
                        file_type="parquet",
                        mime_type="application/parquet",
                        metadata={
                            "original_filename": file.filename,
                            "dataset_id": dataset_id,
                            "converted_from": file_type
                        }
                    )
                
                # Clean up temporary file
                import os
                os.unlink(result["path"])
                
                # Get file path from database
                file_record = await self.repository.get_file(file_id)
                file_path = file_record.file_path
                
                logger.info(f"File created via artifact producer: id={file_id}, path={file_path}")
                # Return file_id, file_path, and a flag indicating artifact producer was used
                return file_id, file_path, True
            
            else:
                # Fallback to original implementation
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
                        file_type="parquet",
                    mime_type="application/parquet",
                    file_data=None,
                    file_size=file_size,
                    file_path=file_path
                )
                file_id = await self.repository.create_file(file_create)
                logger.info(f"File saved to storage: {file_path}, size: {file_size} bytes")
                
                # Return file_id, file_path, and False to indicate artifact producer was NOT used
                return file_id, file_path, False
            
        except Exception as e:
            logger.error(f"Error saving file: {str(e)}")
            raise StorageError("save_file", str(e))
    
    
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
    
    async def _read_parquet_data(
        self,
        file_path: str,
        limit: int,
        offset: int
    ) -> Tuple[List[str], List[Dict[str, Any]], bool]:
        """Read data from Parquet file using DuckDB"""
        logger.info(f"_read_parquet_data called with file_path: {file_path}")
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
    
    # Version tag operations
    async def create_version_tag(
        self,
        dataset_id: int,
        tag_name: str,
        version_id: int
    ) -> int:
        """Create a version tag for a specific dataset version"""
        # Validate dataset exists
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        # Validate version exists and belongs to dataset
        version = await self.get_dataset_version(version_id)
        if not version or version.dataset_id != dataset_id:
            raise DatasetVersionNotFound(version_id)
        
        # Create the tag
        tag_create = VersionTagCreate(
            dataset_id=dataset_id,
            tag_name=tag_name,
            dataset_version_id=version_id
        )
        
        return await self.repository.create_version_tag(tag_create)
    
    async def get_version_tag(self, dataset_id: int, tag_name: str) -> Optional[VersionTag]:
        """Get a version tag by name"""
        return await self.repository.get_version_tag(dataset_id, tag_name)
    
    async def list_version_tags(self, dataset_id: int) -> List[VersionTag]:
        """List all version tags for a dataset"""
        # Validate dataset exists
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        return await self.repository.list_version_tags(dataset_id)
    
    async def delete_version_tag(self, dataset_id: int, tag_name: str) -> bool:
        """Delete a version tag"""
        deleted_id = await self.repository.delete_version_tag(dataset_id, tag_name)
        return deleted_id is not None
    
    # Advanced versioning operations
    async def create_version_from_changes(
        self, 
        request: VersionCreateRequest, 
        user_id: int
    ) -> VersionCreateResponse:
        """Create a new version using overlay-based file changes"""
        # Validate dataset exists
        dataset = await self.get_dataset(request.dataset_id)
        if not dataset:
            raise DatasetNotFound(request.dataset_id)
        
        # Get parent version (latest if not specified)
        if request.parent_version:
            parent_version = await self.get_dataset_version_by_number(
                request.dataset_id, request.parent_version
            )
        else:
            # Get latest version
            resolution = VersionResolution(type=VersionResolutionType.LATEST)
            parent_version = await self.repository.resolve_version(request.dataset_id, resolution)
        
        parent_version_number = parent_version.version_number if parent_version else 0
        
        # Get next version number
        next_version_number = await self.repository.get_next_version_number(request.dataset_id)
        
        # Create overlay data
        overlay_data = OverlayData(
            parent_version=parent_version_number,
            version_number=next_version_number,
            actions=request.file_changes,
            created_at=datetime.now(),
            created_by=user_id,
            message=request.message
        )
        
        # Create overlay file
        overlay_file_id = await self.repository.create_overlay_file(overlay_data)
        
        # Create version record
        version_create = DatasetVersionCreate(
            dataset_id=request.dataset_id,
            version_number=next_version_number,
            overlay_file_id=overlay_file_id,
            created_by=user_id,
            message=request.message
        )
        
        version_id = await self.repository.create_dataset_version(version_create)
        
        # Note: File reference counting removed as part of EDA simplification
        
        # Update dataset timestamp
        await self.repository.update_dataset_timestamp(request.dataset_id)
        
        return VersionCreateResponse(
            version_id=version_id,
            version_number=next_version_number,
            overlay_file_id=overlay_file_id,
        )
    
    async def get_version_by_resolution(
        self, 
        dataset_id: int, 
        resolution: VersionResolution
    ) -> Optional[DatasetVersion]:
        """Get a version using flexible resolution (number, tag, latest)"""
        # Validate dataset exists
        dataset = await self.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)
        
        return await self.repository.resolve_version(dataset_id, resolution)
    
    async def get_dataset_version_by_number(
        self, 
        dataset_id: int, 
        version_number: int
    ) -> Optional[DatasetVersion]:
        """Get a specific version by number"""
        resolution = VersionResolution(
            type=VersionResolutionType.NUMBER, 
            value=version_number
        )
        return await self.get_version_by_resolution(dataset_id, resolution)
    
    async def get_latest_version(self, dataset_id: int) -> Optional[DatasetVersion]:
        """Get the latest version of a dataset"""
        resolution = VersionResolution(type=VersionResolutionType.LATEST)
        return await self.get_version_by_resolution(dataset_id, resolution)
    
    async def get_version_by_tag(
        self, 
        dataset_id: int, 
        tag_name: str
    ) -> Optional[DatasetVersion]:
        """Get a version by tag name"""
        resolution = VersionResolution(
            type=VersionResolutionType.TAG, 
            value=tag_name
        )
        return await self.get_version_by_resolution(dataset_id, resolution)
    
    # Statistics operations
    async def get_version_statistics(self, version_id: int) -> Optional[Dict[str, Any]]:
        """Get pre-computed statistics for a dataset version"""
        stats_data = await self.repository.get_dataset_statistics(version_id)
        
        if not stats_data:
            return None
        
        # Transform the data into the response format
        from app.datasets.models import DatasetStatistics, ColumnStatistics, DatasetStatisticsMetadata
        
        # Parse the JSONB statistics
        statistics = stats_data["statistics"]
        columns = {}
        
        for col_name, col_stats in statistics.get("columns", {}).items():
            columns[col_name] = ColumnStatistics(**col_stats)
        
        metadata = DatasetStatisticsMetadata(**statistics.get("metadata", {
            "profiling_method": "unknown",
            "sampling_applied": False,
            "profiling_duration_ms": 0
        }))
        
        return DatasetStatistics(
            version_id=stats_data["version_id"],
            row_count=stats_data["row_count"],
            column_count=stats_data["column_count"],
            size_bytes=stats_data["size_bytes"],
            size_formatted=StatisticsService.format_size(stats_data["size_bytes"]),
            computed_at=stats_data["computed_at"],
            columns=columns,
            metadata=metadata
        )
    
    async def refresh_version_statistics(
        self, 
        version_id: int, 
        detailed: bool = False,
        sample_size: Optional[int] = None,
        user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Refresh statistics for a dataset version"""
        # Get version info to find the file
        version = await self.get_dataset_version(version_id)
        if not version:
            raise DatasetVersionNotFound(version_id)
        
        # Get the primary file
        primary_file = await self.repository.get_version_file_by_component(
            version_id, "primary", "main"
        )
        
        if not primary_file or not primary_file.file or not primary_file.file.file_path:
            raise FileProcessingError("Primary file not found for version")
        
        file_path = primary_file.file.file_path
        
        # Handle different path formats
        if file_path.startswith("file://"):
            # Extract the actual path from the URI
            from urllib.parse import urlparse
            parsed = urlparse(file_path)
            file_path = parsed.path
        elif not os.path.isabs(file_path):
            # Convert relative path to absolute path
            base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
            file_path = os.path.join(base_path, file_path)
        
        # Create analysis run record
        analysis_run_id = await self.repository.create_analysis_run(
            dataset_version_id=version_id,
            user_id=user_id,
            run_type="profiling",
            run_parameters={
                "method": "detailed_scan" if detailed else "parquet_metadata",
                "detailed": detailed,
                "sample_size": sample_size
            },
            status="running"
        )
        
        try:
            # Calculate statistics
            stats = await StatisticsService.calculate_parquet_statistics(
                file_path, 
                detailed=detailed,
                sample_size=sample_size
            )
            
            # Store in dataset_statistics table
            await self.repository.upsert_dataset_statistics(
                version_id=version_id,
                row_count=stats["row_count"],
                column_count=stats["column_count"],
                size_bytes=stats["size_bytes"],
                statistics=stats["statistics"]
            )
            
            # Update analysis run
            await self.repository.update_analysis_run(
                analysis_run_id=analysis_run_id,
                status="completed",
                execution_time_ms=stats["statistics"]["metadata"]["profiling_duration_ms"],
                output_summary=stats["statistics"]
            )
            
            return {
                "message": "Statistics refresh completed",
                "analysis_run_id": analysis_run_id,
                "status": "completed"
            }
            
        except Exception as e:
            # Update analysis run with failure
            await self.repository.update_analysis_run(
                analysis_run_id=analysis_run_id,
                status="failed",
                output_summary={"error": str(e)}
            )
            
            logger.error(f"Error refreshing statistics for version {version_id}: {str(e)}")
            raise FileProcessingError(f"Failed to refresh statistics: {str(e)}")
    
