"""File parsers for importing data from various formats."""
import os
from typing import List
import pandas as pd

from ..models.file_format import TableData, ParsedData


class CSVParser:
    """Parser for CSV files."""
    
    def can_parse(self, filename: str) -> bool:
        """Check if this parser can handle the given filename."""
        return filename.lower().endswith('.csv')
    
    async def parse(self, file_path: str, filename: str) -> ParsedData:
        """Parse CSV file into structured data."""
        df = pd.read_csv(file_path)
        tables = [TableData(table_key='primary', dataframe=df)]
        
        return ParsedData(
            tables=tables,
            file_type='csv',
            filename=filename
        )
    
    def get_supported_extensions(self) -> List[str]:
        """Return list of file extensions this parser supports."""
        return ['.csv']


class ParquetParser:
    """Parser for Parquet files."""
    
    def can_parse(self, filename: str) -> bool:
        """Check if this parser can handle the given filename."""
        return filename.lower().endswith('.parquet')
    
    async def parse(self, file_path: str, filename: str) -> ParsedData:
        """Parse Parquet file into structured data."""
        df = pd.read_parquet(file_path)
        tables = [TableData(table_key='primary', dataframe=df)]
        
        return ParsedData(
            tables=tables,
            file_type='parquet',
            filename=filename
        )
    
    def get_supported_extensions(self) -> List[str]:
        """Return list of file extensions this parser supports."""
        return ['.parquet']


class ExcelParser:
    """Parser for Excel files (both .xlsx and .xls)."""
    
    def can_parse(self, filename: str) -> bool:
        """Check if this parser can handle the given filename."""
        ext = os.path.splitext(filename)[1].lower()
        return ext in ['.xlsx', '.xls']
    
    async def parse(self, file_path: str, filename: str) -> ParsedData:
        """Parse Excel file into structured data."""
        sheets = pd.read_excel(file_path, sheet_name=None)
        
        tables = []
        for sheet_name, df in sheets.items():
            tables.append(TableData(table_key=sheet_name, dataframe=df))
        
        return ParsedData(
            tables=tables,
            file_type='excel',
            filename=filename
        )
    
    def get_supported_extensions(self) -> List[str]:
        """Return list of file extensions this parser supports."""
        return ['.xlsx', '.xls']