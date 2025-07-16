"""Request and response models for SQL transformation functionality."""

from typing import Optional
from pydantic import BaseModel, Field, validator

from .sql_preview import SqlSource


class SqlTransformTarget(BaseModel):
    """Target configuration for SQL transformation."""
    dataset_id: int = Field(..., description="Dataset ID to update")
    ref: str = Field(..., description="Git ref to update (e.g., 'main')")
    table_key: str = Field(..., description="Table key to update or create")
    message: str = Field(..., description="Commit message for the transformation")
    output_branch_name: Optional[str] = Field(None, description="Name for the output branch (defaults to commit ID)")
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
    target: SqlTransformTarget = Field(..., description="Target configuration for the transformation")
    dry_run: bool = Field(False, description="Whether to validate without executing")
    
    @validator('sources')
    def validate_unique_aliases(cls, v):
        """Ensure all aliases are unique."""
        aliases = [source.alias for source in v]
        if len(aliases) != len(set(aliases)):
            raise ValueError("All source aliases must be unique")
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "sources": [{
                    "alias": "customers",
                    "dataset_id": 123,
                    "ref": "main",
                    "table_key": "customers_table"
                }],
                "sql": "SELECT *, age * 12 as age_months FROM customers",
                "target": {
                    "dataset_id": 123,
                    "ref": "main",
                    "table_key": "customers_enriched",
                    "message": "Added age in months column",
                    "output_branch_name": "transform-2024-01-15",
                    "create_new_dataset": False
                },
                "dry_run": False
            }
        }


class SqlTransformResponse(BaseModel):
    """Response model for SQL transformation endpoint."""
    job_id: str = Field(..., description="Job ID for tracking the transformation")
    status: str = Field(..., description="Initial job status")
    estimated_rows: Optional[int] = Field(None, description="Estimated number of rows to process")
    
    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "pending",
                "estimated_rows": 10234
            }
        }