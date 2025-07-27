"""Data export service implementation for various file formats."""
import csv
import json
from typing import Any, Dict, Iterator, List, Optional
from pathlib import Path
import tempfile
from dataclasses import dataclass
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from openpyxl import Workbook

from src.infrastructure.postgres.table_reader import PostgresTableReader


@dataclass
class ExportOptions:
    """Options for data export."""
    include_headers: bool = True
    compression: Optional[str] = None
    batch_size: int = 10000
    columns: Optional[List[str]] = None
    filters: Optional[Dict[str, Any]] = None


@dataclass
class ExportResult:
    """Result of a data export operation."""
    file_path: str
    content_type: str
    row_count: int
    file_size: int


class DataExportService:
    """Service for exporting data to various formats."""
    
    def __init__(self, table_reader: PostgresTableReader):
        self._table_reader = table_reader
        
    async def export_to_csv(
        self, 
        dataset_id: str, 
        commit_id: str,
        table_name: str,
        options: Optional[ExportOptions] = None
    ) -> ExportResult:
        """Export data to CSV format."""
        options = options or ExportOptions()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            writer = csv.writer(tmp_file)
            row_count = 0
            
            # Write headers if requested
            if options.include_headers:
                columns = await self._get_columns(dataset_id, commit_id, table_name)
                writer.writerow(options.columns or columns)
            
            # Stream data in batches
            async for batch in self._read_batches(dataset_id, commit_id, table_name, options):
                for row in batch:
                    writer.writerow(row)
                    row_count += 1
            
            file_path = tmp_file.name
            file_size = Path(file_path).stat().st_size
            
        return ExportResult(
            file_path=file_path,
            content_type='text/csv',
            row_count=row_count,
            file_size=file_size
        )
    
    async def export_to_excel(
        self,
        dataset_id: str,
        commit_id: str, 
        table_name: str,
        options: Optional[ExportOptions] = None
    ) -> ExportResult:
        """Export data to Excel format."""
        options = options or ExportOptions()
        
        wb = Workbook()
        ws = wb.active
        ws.title = table_name[:31]  # Excel sheet name limit
        
        row_count = 0
        
        # Write headers
        if options.include_headers:
            columns = await self._get_columns(dataset_id, commit_id, table_name)
            ws.append(options.columns or columns)
        
        # Write data in batches
        async for batch in self._read_batches(dataset_id, commit_id, table_name, options):
            for row in batch:
                ws.append(row)
                row_count += 1
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            wb.save(tmp_file.name)
            file_path = tmp_file.name
            file_size = Path(file_path).stat().st_size
            
        return ExportResult(
            file_path=file_path,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            row_count=row_count,
            file_size=file_size
        )
    
    async def export_to_json(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        options: Optional[ExportOptions] = None
    ) -> ExportResult:
        """Export data to JSON format."""
        options = options or ExportOptions()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            columns = await self._get_columns(dataset_id, commit_id, table_name)
            selected_columns = options.columns or columns
            
            tmp_file.write('[')
            row_count = 0
            first_row = True
            
            async for batch in self._read_batches(dataset_id, commit_id, table_name, options):
                for row in batch:
                    if not first_row:
                        tmp_file.write(',')
                    first_row = False
                    
                    row_dict = dict(zip(selected_columns, row))
                    json.dump(row_dict, tmp_file)
                    row_count += 1
            
            tmp_file.write(']')
            file_path = tmp_file.name
            file_size = Path(file_path).stat().st_size
            
        return ExportResult(
            file_path=file_path,
            content_type='application/json',
            row_count=row_count,
            file_size=file_size
        )
    
    async def export_to_parquet(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        options: Optional[ExportOptions] = None
    ) -> ExportResult:
        """Export data to Parquet format."""
        options = options or ExportOptions()
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            file_path = tmp_file.name
            
        # Get schema
        columns = await self._get_columns(dataset_id, commit_id, table_name)
        selected_columns = options.columns or columns
        
        row_count = 0
        writer = None
        
        async for batch in self._read_batches(dataset_id, commit_id, table_name, options):
            # Convert batch to pandas DataFrame
            df = pd.DataFrame(batch, columns=selected_columns)
            
            # Convert to PyArrow table
            table = pa.Table.from_pandas(df)
            
            if writer is None:
                writer = pq.ParquetWriter(file_path, table.schema)
            
            writer.write_table(table)
            row_count += len(batch)
        
        if writer:
            writer.close()
        
        file_size = Path(file_path).stat().st_size
        
        return ExportResult(
            file_path=file_path,
            content_type='application/octet-stream',
            row_count=row_count,
            file_size=file_size
        )
    
    async def _get_columns(self, dataset_id: str, commit_id: str, table_name: str) -> List[str]:
        """Get column names for the table."""
        schema = await self._table_reader.get_table_schema(commit_id, table_name)
        if schema and 'columns' in schema:
            return [col['name'] for col in schema['columns']]
        return []
    
    async def _read_batches(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        options: ExportOptions
    ) -> Iterator[List[tuple]]:
        """Read data in batches."""
        offset = 0
        
        while True:
            # Get data using table reader
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