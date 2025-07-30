"""File format definitions and data models."""
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import pandas as pd


class FileFormat(Enum):
    """Supported file formats for import/export."""
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"
    PARQUET = "parquet"
    
    @property
    def content_type(self) -> str:
        """Get MIME content type for format."""
        content_types = {
            FileFormat.CSV: "text/csv",
            FileFormat.EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            FileFormat.JSON: "application/json",
            FileFormat.PARQUET: "application/octet-stream"
        }
        return content_types.get(self, "application/octet-stream")
    
    @property
    def extension(self) -> str:
        """Get file extension for format."""
        extensions = {
            FileFormat.CSV: ".csv",
            FileFormat.EXCEL: ".xlsx",
            FileFormat.JSON: ".json",
            FileFormat.PARQUET: ".parquet"
        }
        return extensions.get(self, "")


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


@dataclass
class ConversionOptions:
    """Options for file conversion operations."""
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