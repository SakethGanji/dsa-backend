"""Unified file conversion service for import/export operations."""
from typing import Dict, List, Optional, Iterator, Any, AsyncIterator
from pathlib import Path

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.table_reader import PostgresTableReader
from src.core.domain_exceptions import ValidationException
from ..models.file_format import (
    FileFormat, ParsedData, ConversionOptions, ExportResult
)
from .file_parsers import CSVParser, ExcelParser, ParquetParser
from .file_exporters import CSVExporter, ExcelExporter, JSONExporter, ParquetExporter


class FileConversionService:
    """
    Unified service for file format conversions.
    Handles both import (file -> database) and export (database -> file) operations.
    """
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        table_reader: Optional[PostgresTableReader] = None
    ):
        self._uow = uow
        self._table_reader = table_reader
        
        # Initialize parsers
        self._parsers = {
            FileFormat.CSV: CSVParser(),
            FileFormat.EXCEL: ExcelParser(),
            FileFormat.PARQUET: ParquetParser()
        }
        
        # Initialize exporters
        self._exporters = {
            FileFormat.CSV: CSVExporter(),
            FileFormat.EXCEL: ExcelExporter(),
            FileFormat.JSON: JSONExporter(),
            FileFormat.PARQUET: ParquetExporter()
        }
    
    # Import operations (file -> database)
    
    async def import_file(self, file_path: str, filename: str) -> ParsedData:
        """
        Import a file and parse its contents.
        
        Args:
            file_path: Path to the file to import
            filename: Original filename
            
        Returns:
            ParsedData containing parsed tables
        """
        file_format = self._detect_format(filename)
        parser = self._parsers.get(file_format)
        
        if not parser:
            raise ValidationException(
                f"No parser available for format: {file_format}",
                field="file_format"
            )
        
        return await parser.parse(file_path, filename)
    
    def get_supported_import_formats(self) -> List[FileFormat]:
        """Get list of formats supported for import."""
        return list(self._parsers.keys())
    
    # Export operations (database -> file)
    
    async def export_data(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        format: FileFormat,
        options: Optional[ConversionOptions] = None
    ) -> ExportResult:
        """
        Export data from database to file.
        
        Args:
            dataset_id: Dataset to export from
            commit_id: Commit version to export
            table_name: Table to export
            format: Target file format
            options: Export options
            
        Returns:
            ExportResult with file path and metadata
        """
        if not self._table_reader:
            raise ValidationException("Table reader not configured for export operations")
        
        exporter = self._exporters.get(format)
        if not exporter:
            raise ValidationException(
                f"No exporter available for format: {format}",
                field="format"
            )
        
        options = options or ConversionOptions()
        
        # Get columns
        columns = await self._get_columns(dataset_id, commit_id, table_name)
        selected_columns = options.columns or columns
        
        # Create data iterator
        data_iter = self._create_data_iterator(
            dataset_id, commit_id, table_name, options
        )
        
        # Export based on format
        if format == FileFormat.EXCEL:
            return await exporter.export(
                data_iter, selected_columns, options, sheet_name=table_name
            )
        else:
            return await exporter.export(data_iter, selected_columns, options)
    
    def get_supported_export_formats(self) -> List[FileFormat]:
        """Get list of formats supported for export."""
        return list(self._exporters.keys())
    
    # Helper methods
    
    def _detect_format(self, filename: str) -> FileFormat:
        """Detect file format from filename."""
        ext = Path(filename).suffix.lower()
        
        format_map = {
            '.csv': FileFormat.CSV,
            '.xlsx': FileFormat.EXCEL,
            '.xls': FileFormat.EXCEL,
            '.json': FileFormat.JSON,
            '.parquet': FileFormat.PARQUET
        }
        
        format = format_map.get(ext)
        if not format:
            raise ValidationException(
                f"Unsupported file extension: {ext}",
                field="filename"
            )
        
        return format
    
    async def _get_columns(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str
    ) -> List[str]:
        """Get column names for the table."""
        schema = await self._table_reader.get_table_schema(commit_id, table_name)
        if schema and 'columns' in schema:
            return [col['name'] for col in schema['columns']]
        return []
    
    async def _create_data_iterator(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        options: ConversionOptions
    ) -> AsyncIterator[List[List[Any]]]:
        """Create iterator for reading data in batches."""
        offset = 0
        
        while True:
            # Get data batch
            data = await self._table_reader.get_table_data(
                commit_id=commit_id,
                table_key=table_name,
                offset=offset,
                limit=options.batch_size
            )
            
            if not data:
                break
            
            # Apply filters if provided
            if options.filters:
                filtered_data = []
                for row in data:
                    include = True
                    for key, value in options.filters.items():
                        if row.get(key) != value:
                            include = False
                            break
                    if include:
                        filtered_data.append(row)
                data = filtered_data
            
            # Convert to tuples for export
            columns = options.columns or (list(data[0].keys()) if data else [])
            rows = [[row.get(col) for col in columns] for row in data]
            
            yield rows
            offset += options.batch_size
            
            # If we got less than batch_size, we're done
            if len(data) < options.batch_size:
                break