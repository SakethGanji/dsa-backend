"""Request and response models for SQL transformation functionality."""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator

from .sql_preview import SqlSource


class SqlTransformTarget(BaseModel):
    """Target configuration for SQL transformation."""
    dataset_id: int = Field(..., description="Dataset ID to update")
    ref: str = Field(..., description="Git ref to update (e.g., 'main')")
    table_key: str = Field('primary', description="Table key to update or create (always 'primary' for workbench)")
    message: str = Field(..., description="Commit message for the transformation")
    output_branch_name: Optional[str] = Field(None, description="Name for the output branch (defaults to commit ID)")
    expected_head_commit_id: Optional[str] = Field(None, description="Expected commit ID of ref for optimistic locking")
    create_new_dataset: bool = Field(False, description="Whether to create a new dataset")
    new_dataset_name: Optional[str] = Field(None, description="Name for new dataset if creating")
    new_dataset_description: Optional[str] = Field(None, description="Description for new dataset")
    
    @validator('new_dataset_name')
    def validate_new_dataset_name(cls, v, values):
        """Ensure new dataset name is provided when creating new dataset."""
        if values.get('create_new_dataset') and not v:
            raise ValueError("new_dataset_name is required when create_new_dataset is True")
        return v


class SqlTransformRequest(BaseModel):
    """Request model for SQL transformation endpoint."""
    sources: list[SqlSource] = Field(..., min_items=1, description="Source tables for the query")
    sql: str = Field(..., min_length=1, description="SQL query to execute for transformation")
    target: Optional[SqlTransformTarget] = Field(None, description="Target configuration for the transformation (required when save=True)")
    dry_run: bool = Field(False, description="Whether to validate without executing")
    save: bool = Field(False, description="Whether to save results (True) or preview only (False)")
    limit: int = Field(1000, ge=1, le=10000, description="Maximum number of rows to return (for preview mode)")
    offset: int = Field(0, ge=0, description="Number of rows to skip (for preview mode)")
    quick_preview: bool = Field(False, description="Use sampling for faster preview (approximate results)")
    sample_percent: float = Field(1.0, gt=0, le=100, description="Percentage of rows to sample when quick_preview=true (0.1-100)")
    
    @validator('sources')
    def validate_unique_aliases(cls, v):
        """Ensure all aliases are unique."""
        aliases = [source.alias for source in v]
        if len(aliases) != len(set(aliases)):
            raise ValueError("All source aliases must be unique")
        return v
    
    @validator('target', always=True)
    def validate_target(cls, v, values):
        """Ensure target is provided when save=True."""
        # Only validate if save is explicitly True
        if values.get('save') is True and v is None:
            raise ValueError("target is required when save is True")
        return v
    
    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "title": "Save mode (create transformation)",
                    "value": {
                        "sources": [{
                            "alias": "customers",
                            "dataset_id": 123,
                            "ref": "main",
                            "table_key": "primary"
                        }],
                        "sql": "SELECT *, age * 12 as age_months FROM customers",
                        "target": {
                            "dataset_id": 123,
                            "ref": "main",
                            "table_key": "primary",
                            "message": "Added age in months column",
                            "output_branch_name": "age-months-transform",
                            "create_new_dataset": False
                        },
                        "save": True,
                        "dry_run": False
                    }
                },
                {
                    "title": "Preview mode (no save)",
                    "value": {
                        "sources": [{
                            "alias": "customers",
                            "dataset_id": 123,
                            "ref": "main",
                            "table_key": "primary"
                        }],
                        "sql": "SELECT * FROM customers WHERE age > 25",
                        "save": False,
                        "limit": 100,
                        "offset": 0
                    }
                }
            ]
        }


class SqlTransformResponse(BaseModel):
    """Response model for SQL transformation endpoint."""
    # Fields for save mode (when save=True)
    job_id: Optional[str] = Field(None, description="Job ID for tracking the transformation (when save=True)")
    status: Optional[str] = Field(None, description="Initial job status (when save=True)")
    estimated_rows: Optional[int] = Field(None, description="Estimated number of rows to process (when save=True)")
    
    # Fields for preview mode (when save=False)
    data: Optional[List[Dict[str, Any]]] = Field(None, description="Query result rows (when save=False)")
    row_count: Optional[int] = Field(None, description="Number of rows returned (when save=False)")
    total_row_count: Optional[int] = Field(None, description="Total rows available (when save=False)")
    execution_time_ms: Optional[int] = Field(None, description="Query execution time in milliseconds (when save=False)")
    columns: Optional[List[Dict[str, str]]] = Field(None, description="Column names and types (when save=False)")
    has_more: Optional[bool] = Field(None, description="Whether more rows are available (when save=False)")
    
    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "title": "Save mode response",
                    "value": {
                        "job_id": "550e8400-e29b-41d4-a716-446655440000",
                        "status": "pending",
                        "estimated_rows": 10234
                    }
                },
                {
                    "title": "Preview mode response",
                    "value": {
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
                        "has_more": True
                    }
                }
            ]
        }