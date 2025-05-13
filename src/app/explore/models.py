from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

class ProfileFormat(str, Enum):
    """Format options for the profile output"""
    JSON = "json"
    HTML = "html"

class Operation(BaseModel):
    """Base model for dataset operation"""
    type: str = Field(..., description="Type of operation to perform")

class FilterOperation(Operation):
    """Filter rows operation"""
    type: str = "filter_rows"
    expression: str = Field(..., description="Pandas query expression to filter rows")

class SampleOperation(Operation):
    """Sample rows operation"""
    type: str = "sample_rows"
    fraction: float = Field(0.1, description="Fraction of rows to sample (0-1)")
    method: str = Field("random", description="Sampling method: random, head, or tail")

class RemoveColumnsOperation(Operation):
    """Remove columns operation"""
    type: str = "remove_columns"
    columns: List[str] = Field(..., description="List of column names to remove")

class RenameColumnsOperation(Operation):
    """Rename columns operation"""
    type: str = "rename_columns"
    mappings: Dict[str, str] = Field(..., description="Mapping of old column names to new names")

class RemoveNullsOperation(Operation):
    """Remove rows with null values operation"""
    type: str = "remove_nulls"
    columns: Optional[List[str]] = Field(None, description="List of columns to check for nulls (all columns if None)")

class DeriveColumnOperation(Operation):
    """Create a derived column operation"""
    type: str = "derive_column"
    column: str = Field(..., description="Name of the new column")
    expression: str = Field(..., description="Expression to evaluate (with df as the DataFrame)")

class SortRowsOperation(Operation):
    """Sort rows operation"""
    type: str = "sort_rows"
    columns: List[str] = Field(..., description="List of columns to sort by")
    order: List[str] = Field(default_factory=lambda: ["asc"], description="Sort order for each column (asc or desc)")

class ExploreRequest(BaseModel):
    """
    Request model for the explore endpoint.
    Contains operations to apply to the dataset before profiling.
    """
    operations: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of operations to apply to the dataset"
    )
    sheet: Optional[str] = Field(
        None, 
        description="Optional sheet name for Excel files"
    )
    format: ProfileFormat = Field(
        ProfileFormat.JSON,
        description="Output format for the profile report (json or html)"
    )
    run_profiling: bool = Field(
        False,
        description="Whether to run full profiling (may be slow for large datasets)"
    )
    
class ProfileResponse(BaseModel):
    """
    Response model for the explore endpoint.
    Contains the profiling report.
    """
    format: str = Field(..., description="Format of the profile (json or html)")
    summary: Dict[str, Any] = Field(..., description="Summary of the dataset")
    profile: Optional[Dict[str, Any]] = Field(None, description="Profiling report (if run_profiling is True)")