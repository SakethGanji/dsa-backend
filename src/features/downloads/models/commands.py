"""Downloads command objects."""

from dataclasses import dataclass
from typing import Optional, List


@dataclass
class DownloadDatasetCommand:
    """Command to download dataset data."""
    user_id: int
    dataset_id: int
    ref_name: str
    format: str = "csv"  # csv, excel, json
    table_key: Optional[str] = None  # If None, download all tables


@dataclass
class DownloadTableCommand:
    """Command to download table data."""
    user_id: int
    dataset_id: int
    ref_name: str
    table_key: str
    format: str = "csv"  # csv or json
    columns: Optional[List[str]] = None  # Specific columns to include