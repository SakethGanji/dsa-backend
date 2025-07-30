"""Request and response models for SQL preview functionality."""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, validator


class SqlSource(BaseModel):
    """Represents a source table for SQL query."""
    alias: str = Field(..., description="Table alias to use in SQL query")
    dataset_id: int = Field(..., description="Dataset ID")
    ref: str = Field(..., description="Git ref name (e.g., 'main')")
    table_key: str = Field(..., description="Table key within the dataset")
    
    @validator('alias')
    def validate_alias(cls, v):
        """Ensure alias is a valid SQL identifier."""
        if not v or not v[0].isalpha():
            raise ValueError("Alias must start with a letter")
        if not all(c.isalnum() or c == '_' for c in v):
            raise ValueError("Alias must contain only letters, numbers, and underscores")
        return v


class SqlPreviewRequest(BaseModel):
    """Request model for SQL preview endpoint."""
    sources: List[SqlSource] = Field(..., min_items=1, description="Source tables for the query")
    sql: str = Field(..., min_length=1, description="SQL query to execute")
    limit: int = Field(1000, ge=1, le=10000, description="Maximum number of rows to return")
    
    @validator('sources')
    def validate_unique_aliases(cls, v):
        """Ensure all aliases are unique."""
        aliases = [source.alias for source in v]
        if len(aliases) != len(set(aliases)):
            raise ValueError("All source aliases must be unique")
        return v


class SqlPreviewResponse(BaseModel):
    """Response model for SQL preview endpoint."""
    data: List[Dict[str, Any]] = Field(..., description="Query result rows")
    row_count: int = Field(..., description="Number of rows returned")
    total_row_count: Optional[int] = Field(None, description="Total rows if known")
    execution_time_ms: int = Field(..., description="Query execution time in milliseconds")
    columns: List[Dict[str, str]] = Field(..., description="Column names and types")
    truncated: bool = Field(False, description="Whether results were truncated due to limit")
    
    class Config:
        json_schema_extra = {
            "example": {
                "data": [
                    {"id": 1, "name": "John Doe", "age": 30},
                    {"id": 2, "name": "Jane Smith", "age": 25}
                ],
                "row_count": 2,
                "total_row_count": 100,
                "execution_time_ms": 234,
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "age", "type": "INTEGER"}
                ],
                "truncated": False
            }
        }