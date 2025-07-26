"""Concrete implementations of file parsers."""

import os
from typing import List
import pandas as pd

from dataclasses import dataclass
import pandas as pd
from typing import List

# Data classes for file processing
@dataclass
class TableData:
    """Represents data from a single table/sheet."""
    table_key: str  # 'primary' for single-table formats, sheet name for Excel
    dataframe: pd.DataFrame
    
@dataclass
class ParsedData:
    """Result of parsing a file containing one or more tables."""
    tables: List[TableData]
    file_type: str
    filename: str


class CSVParser:
    """Parser for CSV files."""
    
    def can_parse(self, filename: str) -> bool:
        """Check if this parser can handle the given filename."""
        return filename.lower().endswith('.csv')
    
    async def parse(self, file_path: str, filename: str) -> ParsedData:
        """Parse CSV file into structured data."""
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # CSV files have a single table with 'primary' key
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
        # Read Parquet file
        df = pd.read_parquet(file_path)
        
        # Parquet files have a single table with 'primary' key
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
        # Read all sheets from Excel file
        sheets = pd.read_excel(file_path, sheet_name=None)
        
        # Create TableData for each sheet
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