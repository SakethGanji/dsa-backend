"""File exporters for exporting data to various formats."""
import csv
import json
from typing import Any, Dict, Iterator, List, Optional, AsyncIterator
from pathlib import Path
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from openpyxl import Workbook

from ..models.file_format import ConversionOptions, ExportResult


class CSVExporter:
    """Exporter for CSV format."""
    
    async def export(
        self,
        data_iterator: AsyncIterator[List[List[Any]]],
        columns: List[str],
        options: ConversionOptions
    ) -> ExportResult:
        """Export data to CSV format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            writer = csv.writer(tmp_file)
            row_count = 0
            
            if options.include_headers:
                writer.writerow(columns)
            
            async for batch in data_iterator:
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


class ExcelExporter:
    """Exporter for Excel format."""
    
    async def export(
        self,
        data_iterator: AsyncIterator[List[List[Any]]],
        columns: List[str],
        options: ConversionOptions,
        sheet_name: str = "Data"
    ) -> ExportResult:
        """Export data to Excel format."""
        wb = Workbook()
        ws = wb.active
        ws.title = sheet_name[:31]  # Excel sheet name limit
        
        row_count = 0
        
        if options.include_headers:
            ws.append(columns)
        
        async for batch in data_iterator:
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


class JSONExporter:
    """Exporter for JSON format."""
    
    async def export(
        self,
        data_iterator: AsyncIterator[List[List[Any]]],
        columns: List[str],
        options: ConversionOptions
    ) -> ExportResult:
        """Export data to JSON format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            tmp_file.write('[')
            row_count = 0
            first_row = True
            
            async for batch in data_iterator:
                for row_data in batch:
                    if not first_row:
                        tmp_file.write(',')
                    first_row = False
                    
                    row_dict = dict(zip(columns, row_data))
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


class ParquetExporter:
    """Exporter for Parquet format."""
    
    async def export(
        self,
        data_iterator: AsyncIterator[List[List[Any]]],
        columns: List[str],
        options: ConversionOptions
    ) -> ExportResult:
        """Export data to Parquet format."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            file_path = tmp_file.name
            
        row_count = 0
        writer = None
        
        async for batch in data_iterator:
            df = pd.DataFrame(batch, columns=columns)
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