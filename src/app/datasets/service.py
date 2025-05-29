from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import os
import logging
from io import BytesIO
from fastapi import UploadFile, HTTPException
from app.datasets.repository import DatasetsRepository
from app.datasets.duckdb_service import DuckDBService
from app.datasets.models import (
    Dataset, DatasetCreate, DatasetUpdate, DatasetUploadRequest, DatasetUploadResponse,
    DatasetVersion, DatasetVersionCreate, File, FileCreate,
    Sheet, SheetCreate, SheetInfo, SheetMetadata, Tag
)

logger = logging.getLogger(__name__)

class DatasetsService:
    def __init__(self, repository: DatasetsRepository):
        self.repository = repository

    async def upload_dataset(
        self, 
        file: UploadFile, 
        request: DatasetUploadRequest, 
        user_id: int
    ) -> DatasetUploadResponse:
        """
        Process dataset upload following the flow:
        1. Create/Upsert dataset
        2. Save file
        3. Get next version number
        4. Create dataset version
        5. Update dataset timestamp
        6. Process tags if any
        7. Parse file into sheets
        8. Create sheets and metadata
        9. Return response with dataset info
        """
        # Step 0: Upsert dataset if dataset_id provided, otherwise create new dataset
        dataset_create = DatasetCreate(
            name=request.name,
            description=request.description,
            created_by=user_id,
            tags=request.tags
        )
        
        dataset_id = await self.repository.upsert_dataset(request.dataset_id, dataset_create)
        
        # Step 1: Check file size before processing
        # Read file content once to get size and data
        contents = await file.read()
        file_size = len(contents)
        
        # Reset file position for potential re-reading
        await file.seek(0)
        
        # Define size limits
        MAX_MEMORY_SIZE = 100 * 1024 * 1024  # 100MB for in-memory storage
        MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB max file size
        
        if file_size > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"File size {file_size / (1024**3):.2f}GB exceeds maximum allowed size of {MAX_FILE_SIZE / (1024**3):.2f}GB"
            )
        
        file_type = os.path.splitext(file.filename)[1].lower()[1:]  # Remove the dot
        
        # For large files, use file system storage
        if file_size > MAX_MEMORY_SIZE:
            # Use filesystem storage for large files
            file_id = await self._save_large_file(file, contents, file_type, file_size)
        else:
            # Use database storage for smaller files
            logger.info(f"Saving file to database - Size: {file_size} bytes, Type: {file_type}")
            
            if len(contents) == 0:
                raise HTTPException(
                    status_code=400,
                    detail="File is empty or could not be read"
                )
            
            file_create = FileCreate(
                storage_type="database",
                file_type=file_type,
                mime_type=file.content_type,
                file_data=contents,
                file_size=file_size
            )
            file_id = await self.repository.create_file(file_create)
            logger.info(f"File saved to database with ID: {file_id}")
        
        # Step 2: Get next version number
        version_number = await self.repository.get_next_version_number(dataset_id)
        
        # Step 3: Create dataset version
        version_create = DatasetVersionCreate(
            dataset_id=dataset_id,
            version_number=version_number,
            file_id=file_id,
            uploaded_by=user_id
        )
        
        version_id = await self.repository.create_dataset_version(version_create)
        
        # Step 4: Update dataset timestamp
        await self.repository.update_dataset_timestamp(dataset_id)
        
        # Step 5: Process tags if any
        if request.tags:
            for tag_name in request.tags:
                tag_id = await self.repository.upsert_tag(tag_name)
                await self.repository.create_dataset_tag(dataset_id, tag_id)
        
        # Step 6: Parse file into sheets
        # For large files, we need to get the file info from storage
        file_info = await self.repository.get_file(file_id)
        
        # Debug logging
        logger.info(f"File info retrieved - ID: {file_info.id if file_info else 'None'}, "
                   f"Storage type: {file_info.storage_type if file_info else 'None'}, "
                   f"File type: {file_info.file_type if file_info else 'None'}, "
                   f"Has file_data: {file_info.file_data is not None if file_info else False}")
        
        sheet_infos = await self._parse_file_into_sheets(
            file=file, 
            file_info=file_info, 
            file_type=file_type, 
            version_id=version_id
        )
        
        # Step 8: Return response with dataset info
        return DatasetUploadResponse(
            dataset_id=dataset_id,
            version_id=version_id,
            sheets=sheet_infos
        )

    async def _save_large_file(self, file: UploadFile, contents: bytes, file_type: str, file_size: int) -> int:
        """Save large file to filesystem and create a reference in database"""
        import uuid
        
        # Create a unique filename
        file_uuid = str(uuid.uuid4())
        storage_path = f"/tmp/dsa_files/{file_uuid}.{file_type}"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(storage_path), exist_ok=True)
        
        # Write contents to disk
        try:
            with open(storage_path, 'wb') as buffer:
                buffer.write(contents)
            
            # Create file record with filesystem reference
            file_create = FileCreate(
                storage_type="filesystem",
                file_type=file_type,
                mime_type=file.content_type,
                file_data=None,  # No data in DB
                file_size=file_size,
                file_path=storage_path  # Store path reference
            )
            
            return await self.repository.create_file(file_create)
        except Exception as e:
            # Clean up on error
            if os.path.exists(storage_path):
                os.unlink(storage_path)
            raise e
    
    async def _parse_file_into_sheets(
        self,
        file: UploadFile,
        file_info: Any,
        file_type: str,
        version_id: int
    ) -> List[SheetInfo]:
        """Parse file contents into sheets based on file type"""
        sheet_infos = []
        
        try:
            filename = os.path.basename(file.filename)
            parser = self._get_file_parser(file_type)
            sheet_infos = await parser(file_info, filename, version_id)
        except Exception as e:
            # Log the error and create an error sheet
            error_msg = f"Error parsing file: {str(e)}"
            logger.error(f"Failed to parse {file.filename} (type: {file_type}, version_id: {version_id})", exc_info=True)
            sheet_infos = await self._create_error_sheet(file, version_id, file_type, error_msg)
            
        return sheet_infos
    
    def _get_file_parser(self, file_type: str):
        """Factory method to get the appropriate file parser"""
        parsers = {
            "xlsx": self._parse_excel_file_duckdb,
            "xls": self._parse_excel_file_duckdb,
            "xlsm": self._parse_excel_file_duckdb,
            "csv": self._parse_csv_file_duckdb
        }
        return parsers.get(file_type, self._handle_unsupported_file_type)
    
    async def _parse_excel_file_duckdb(self, file_info: Any, filename: str, version_id: int) -> List[SheetInfo]:
        """Parse Excel file into sheets using DuckDB"""
        sheet_infos = []
        
        # Use DuckDB service to parse the Excel file
        duckdb_service = DuckDBService()
        parsed_sheets = await duckdb_service.parse_excel_file_from_info(file_info, filename)
        
        for sheet_data in parsed_sheets:
            sheet_create = SheetCreate(
                dataset_version_id=version_id,
                name=sheet_data['name'],
                sheet_index=sheet_data['index'],
                description=None
            )

            sheet_id = await self.repository.create_sheet(sheet_create)

            # Create sheet metadata from DuckDB results
            await self.repository.create_sheet_metadata(sheet_id, sheet_data['metadata'])

            sheet_infos.append(SheetInfo(
                id=sheet_id,
                name=sheet_data['name'],
                index=sheet_data['index'],
                description=None
            ))
            
        return sheet_infos
    
    async def _parse_csv_file_duckdb(self, file_info: Any, filename: str, version_id: int) -> List[SheetInfo]:
        """Parse CSV file into a sheet using DuckDB"""
        # Use DuckDB service to parse the CSV file
        duckdb_service = DuckDBService()
        parsed_sheets = await duckdb_service.parse_csv_file_from_info(file_info, filename)
        
        sheet_data = parsed_sheets[0]  # CSV has only one sheet
        
        sheet_create = SheetCreate(
            dataset_version_id=version_id,
            name=sheet_data['name'],
            sheet_index=0,
            description=None
        )

        sheet_id = await self.repository.create_sheet(sheet_create)

        # Create sheet metadata from DuckDB results
        await self.repository.create_sheet_metadata(sheet_id, sheet_data['metadata'])

        return [SheetInfo(
            id=sheet_id,
            name=sheet_data['name'],
            index=0,
            description=None
        )]
    
    async def _handle_unsupported_file_type(self, file_info: Any, filename: str, version_id: int) -> List[SheetInfo]:
        """Handle unsupported file types"""
        file_type = os.path.splitext(filename)[1].lower()[1:]
        sheet_create = SheetCreate(
            dataset_version_id=version_id,
            name=filename,
            sheet_index=0,
            description=f"Unsupported file type: {file_type}"
        )

        sheet_id = await self.repository.create_sheet(sheet_create)

        # Create minimal metadata
        metadata = {
            "file_type": file_type,
            "note": "Unsupported file type - no parsing performed"
        }

        await self.repository.create_sheet_metadata(sheet_id, metadata)

        return [SheetInfo(
            id=sheet_id,
            name=filename,
            index=0,
            description=f"Unsupported file type: {file_type}"
        )]
    
    async def _create_error_sheet(self, file: UploadFile, version_id: int, file_type: str, error_msg: str) -> List[SheetInfo]:
        """Create a sheet entry for a file parsing error"""
        filename = os.path.basename(file.filename)
        sheet_create = SheetCreate(
            dataset_version_id=version_id,
            name=filename,
            sheet_index=0,
            description="Error parsing file"
        )

        sheet_id = await self.repository.create_sheet(sheet_create)

        # Create error metadata
        metadata = {
            "error": error_msg,
            "file_type": file_type
        }

        await self.repository.create_sheet_metadata(sheet_id, metadata)

        return [SheetInfo(
            id=sheet_id,
            name=filename,
            index=0,
            description="Error parsing file"
        )]
    
    async def _generate_profile_report(self, data_source, sheet_name=None):
        """
        Generate profiling report for a dataset.
        This is a stub method to be implemented when profiling is needed.

        Args:
            data_source: The data source (Excel file, DataFrame, etc.)
            sheet_name: Optional sheet name for Excel files

        Returns:
            int: The file_id of the saved profiling report
        """
        # Implementation for profiling data goes here
        # For example, using pandas-profiling or other libraries
        # Create a report file and save it using self.repository.create_file
        # Return the file_id

        # Placeholder implementation
        return None

    # Dataset listing and retrieval methods
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
        created_at_from: Optional[datetime] = None,
        created_at_to: Optional[datetime] = None,
        updated_at_from: Optional[datetime] = None,
        updated_at_to: Optional[datetime] = None
    ) -> List[Dataset]:
        """List datasets with optional filtering, sorting, and pagination"""
        # Validate and normalize inputs
        if limit < 1:
            limit = 10
        elif limit > 100:
            limit = 100

        if offset < 0:
            offset = 0

        # Default sort parameters
        valid_sort_fields = ["name", "created_at", "updated_at", "file_size", "current_version"]
        if sort_by not in valid_sort_fields:
            sort_by = "updated_at"

        valid_sort_orders = ["asc", "desc"]
        if sort_order not in valid_sort_orders:
            sort_order = "desc"
        
        # Call repository method
        result = await self.repository.list_datasets(
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
            created_at_from=created_at_from,
            created_at_to=created_at_to,
            updated_at_from=updated_at_from,
            updated_at_to=updated_at_to
        )

        # Transform raw results into appropriate response models
        # The repository now returns List[Dataset], so direct transformation might not be needed
        # or needs to be adjusted if the structure from repository.list_datasets is already Pydantic models.
        # Assuming result is List[Dataset] as per repository changes.
        return result

    async def get_dataset(self, dataset_id: int) -> Optional[Dataset]:
        """Get detailed information about a single dataset"""
        result = await self.repository.get_dataset(dataset_id)
        # No transformation needed if repository returns Optional[Dataset]
        return result

    async def update_dataset(self, dataset_id: int, data: DatasetUpdate) -> Optional[Dataset]:
        """Update dataset metadata including name, description, and tags"""
        # First check if dataset exists
        # get_dataset now returns Optional[Dataset]
        existing_dataset = await self.get_dataset(dataset_id)
        if not existing_dataset:
            return None

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

    async def list_dataset_versions(self, dataset_id: int) -> List[DatasetVersion]:
        """List all versions of a dataset"""
        # Check if dataset exists
        dataset = await self.get_dataset(dataset_id) # Returns Optional[Dataset]
        if not dataset:
            return [] # Return empty list if dataset not found

        result = await self.repository.list_dataset_versions(dataset_id)
        # No transformation needed if repository returns List[DatasetVersion]
        return result

    async def get_dataset_version(self, version_id: int) -> Optional[DatasetVersion]:
        """Get detailed information about a single dataset version"""
        result = await self.repository.get_dataset_version(version_id)
        # No transformation needed if repository returns Optional[DatasetVersion]
        # JSON parsing for sheets should ideally be handled within the repository or model itself if possible.
        # However, if sheets are part of the DatasetVersion Pydantic model and are complex,
        # the repository should ensure they are correctly populated.
        return result

    async def get_dataset_version_file(self, version_id: int) -> Optional[File]:
        """Get file information for a dataset version"""
        # Get the version first
        version = await self.get_dataset_version(version_id) # Returns Optional[DatasetVersion]
        if not version or not version.file_id: # Check if version and file_id exist
            return None

        # Get the associated file
        file_info = await self.repository.get_file(version.file_id) # Returns Optional[File]
        return file_info

    async def delete_dataset_version(self, version_id: int) -> bool:
        """Delete a dataset version"""
        # Check if version exists
        version = await self.get_dataset_version(version_id)
        if not version:
            return False

        deleted_id = await self.repository.delete_dataset_version(version_id)
        return deleted_id is not None

    async def list_tags(self) -> List[Tag]:
        """List all available tags"""
        result = await self.repository.list_tags()
        # No transformation needed if repository returns List[Tag]
        return result

    async def list_version_sheets(self, version_id: int) -> List[Sheet]:
        """Get all sheets for a dataset version"""
        # First check if version exists
        version = await self.get_dataset_version(version_id) # Returns Optional[DatasetVersion]
        if not version:
            return []

        # Get sheets
        sheets = await self.repository.list_version_sheets(version_id) # Returns List[Sheet]
        return sheets

    async def get_sheet_data(self, version_id: int, sheet_name: str, limit: int = 100, offset: int = 0) -> Tuple[List[str], List[Dict[str, Any]], bool]:
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

        # For CSV, there should be just one sheet, use that
        if file_info.file_type == "csv" and not sheet_name and sheets:
            sheet_name = sheets[0].name

        # Validate sheet name
        if sheet_name not in sheet_names:
            return [], [], False

        # Get the data based on file type and storage type
        try:
            duckdb_service = DuckDBService()
            return await duckdb_service.get_sheet_data_from_file_info(file_info, sheet_name, limit, offset)
        except Exception as e:
            logger.error(f"Error reading file data: {str(e)}")
            return [], [], False

