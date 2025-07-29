"""Commands for file conversion operations."""
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from src.features.base_handler import Command
from .file_format import FileFormat


@dataclass
class ImportFileCommand(Command):
    """Command to import a file."""
    file_path: str
    filename: str
    dataset_id: Optional[str] = None  # Target dataset if known


@dataclass
class ExportDataCommand(Command):
    """Command to export data to a file."""
    dataset_id: str
    ref_name: str
    format: FileFormat
    table_key: str = "primary"
    include_headers: bool = True
    columns: Optional[List[str]] = None
    filters: Optional[Dict[str, Any]] = None