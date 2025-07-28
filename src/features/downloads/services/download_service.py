"""Consolidated service for all download operations."""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import io
import csv
import json

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.table_reader import PostgresTableReader
from .data_export_service import DataExportService, ExportOptions
from src.core.domain_exceptions import EntityNotFoundException, ValidationException
from ...base_handler import with_error_handling
from ..models import DownloadDatasetCommand, DownloadTableCommand


@dataclass
class DownloadResponse:
    """Response for download operations."""
    content: bytes
    content_type: str
    filename: str
    metadata: Optional[Dict[str, Any]] = None


class DownloadService:
    """Consolidated service for all download operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        table_reader: PostgresTableReader,
        export_service: Optional[DataExportService] = None
    ):
        self._uow = uow
        self._table_reader = table_reader
        self._export_service = export_service
    
    @with_error_handling
    async def download_dataset(
        self,
        command: DownloadDatasetCommand
    ) -> DownloadResponse:
        """Download entire dataset in specified format."""
        # Validate format
        valid_formats = ["csv", "excel", "json", "parquet"]
        if command.format not in valid_formats:
            raise ValidationException(
                f"Unsupported format: {command.format}. Valid formats: {valid_formats}",
                field="format"
            )
        
        # Get dataset and validate access
        dataset = await self._uow.datasets.get_dataset_by_id(command.dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Get ref to find commit
        ref = await self._uow.commits.get_ref(command.dataset_id, command.ref_name)
        if not ref:
            raise EntityNotFoundException("Ref", command.ref_name)
        
        # Ensure export service is available
        if not self._export_service:
            from src.features.downloads.services.data_export_service import DataExportService as ExportServiceClass
            self._export_service = ExportServiceClass(self._table_reader)
        
        # Prepare export options
        options = ExportOptions(
            include_headers=command.include_headers if hasattr(command, 'include_headers') else True,
            columns=command.columns if hasattr(command, 'columns') else None,
            filters=command.filters if hasattr(command, 'filters') else None
        )
        
        # Determine table to export
        table_name = command.table_key or "primary"
        
        # Export using service
        export_methods = {
            "csv": self._export_service.export_to_csv,
            "excel": self._export_service.export_to_excel,
            "json": self._export_service.export_to_json,
            "parquet": self._export_service.export_to_parquet
        }
        
        export_method = export_methods[command.format]
        result = await export_method(
            dataset_id=command.dataset_id,
            commit_id=ref['commit_id'],
            table_name=table_name,
            options=options
        )
        
        # Read file content
        with open(result.file_path, 'rb') as f:
            content = f.read()
        
        # Return formatted response
        return DownloadResponse(
            content=content,
            content_type=result.content_type,
            filename=f"{dataset['name']}.{command.format}",
            metadata={
                "row_count": result.row_count,
                "file_size": result.file_size
            }
        )
    
    @with_error_handling
    async def download_table(
        self,
        command: DownloadTableCommand
    ) -> DownloadResponse:
        """Download a specific table in specified format."""
        # Validate format
        if command.format not in ["csv", "json"]:
            raise ValidationException(f"Unsupported format: {command.format}", field="format")
        
        # Get dataset info
        dataset = await self._uow.datasets.get_dataset_by_id(command.dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Get ref
        ref = await self._uow.commits.get_ref(command.dataset_id, command.ref_name)
        if not ref:
            raise EntityNotFoundException("Ref", command.ref_name)
        
        commit_id = ref['commit_id']
        
        # Get all data in batches
        all_data = []
        offset = 0
        batch_size = 1000
        
        while True:
            result = await self._table_reader.get_table_data(
                commit_id=commit_id,
                table_key=command.table_key,
                offset=offset,
                limit=batch_size
            )
            
            # Filter columns if specified
            if command.columns and result:
                filtered_result = []
                for row in result:
                    # Keep _logical_row_id plus requested columns
                    filtered_row = {k: v for k, v in row.items() 
                                  if k in command.columns or k == '_logical_row_id'}
                    filtered_result.append(filtered_row)
                result = filtered_result
            
            all_data.extend(result)
            offset += batch_size
            
            if len(result) < batch_size:
                break
        
        # Format data based on requested format
        if command.format == "csv":
            content = await self._format_as_csv(
                data=all_data,
                dataset_name=dataset['name'],
                table_key=command.table_key,
                commit_id=commit_id
            )
            content_type = "text/csv"
        else:  # json
            content = await self._format_as_json(
                data=all_data,
                dataset_name=dataset['name'],
                table_key=command.table_key,
                commit_id=commit_id,
                include_schema=True
            )
            content_type = "application/json"
        
        filename = f"{dataset['name']}_{command.table_key}_{commit_id[:8]}.{command.format}"
        
        return DownloadResponse(
            content=content,
            content_type=content_type,
            filename=filename
        )
    
    async def _format_as_csv(
        self, 
        data: List[Dict], 
        dataset_name: str,
        table_key: str,
        commit_id: str
    ) -> bytes:
        """Format data as CSV."""
        output = io.StringIO()
        
        if data:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        
        return output.getvalue().encode('utf-8')
    
    async def _format_as_json(
        self,
        data: List[Dict],
        dataset_name: str,
        table_key: str,
        commit_id: str,
        include_schema: bool = True
    ) -> bytes:
        """Format data as JSON with optional schema."""
        result = {
            "dataset_name": dataset_name,
            "commit_id": commit_id,
            "table_key": table_key,
            "row_count": len(data),
            "data": data
        }
        
        if include_schema:
            # Get schema
            schema = await self._table_reader.get_table_schema(commit_id, table_key)
            result["schema"] = schema
        
        return json.dumps(result, indent=2, default=str).encode('utf-8')