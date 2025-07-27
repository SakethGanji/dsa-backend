"""Handler for downloading datasets in various formats."""
from typing import Dict, Any

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.services import DataExportService, ExportOptions
from src.core.domain_exceptions import EntityNotFoundException, ValidationException
from ..models import DownloadDatasetCommand


class DownloadDatasetHandler:
    """Handler for downloading dataset data."""
    
    def __init__(self, uow: PostgresUnitOfWork, export_service: DataExportService):
        self._uow = uow
        self._export_service = export_service
    
    async def handle(self, command: DownloadDatasetCommand) -> Dict[str, Any]:
        """Download dataset data in specified format."""
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
        return {
            "content": content,
            "content_type": result.content_type,
            "filename": f"{dataset['name']}.{command.format}",
            "metadata": {
                "row_count": result.row_count,
                "file_size": result.file_size
            }
        }