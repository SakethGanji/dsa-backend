"""API Request models."""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime
from uuid import UUID


# ============================================
# Dataset Request Models
# ============================================

class CreateDatasetRequest(BaseModel):
    """Request to create a new dataset."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    tags: List[str] = Field(default_factory=list)


class UpdateDatasetRequest(BaseModel):
    """Request to update a dataset."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    tags: Optional[List[str]] = None


# ============================================
# User Request Models
# ============================================

class CreateUserRequest(BaseModel):
    """Request to create a new user."""
    soeid: str = Field(..., min_length=1, max_length=50)
    role_id: int = Field(..., ge=1)
    is_active: bool = Field(True)


class UpdateUserRequest(BaseModel):
    """Request to update a user."""
    role_id: Optional[int] = Field(None, ge=1)
    is_active: Optional[bool] = None


class LoginRequest(BaseModel):
    """Request to login."""
    soeid: str = Field(..., min_length=1, max_length=50)
    password: str = Field(..., min_length=1)


# ============================================
# Job Request Models
# ============================================

class QueueImportRequest(BaseModel):
    """Request to queue an import job."""
    commit_message: str = Field(..., min_length=1, max_length=500)


class CancelJobRequest(BaseModel):
    """Request to cancel a job."""
    reason: Optional[str] = None


# ============================================
# Version Control Request Models
# ============================================

class CreateCommitRequest(BaseModel):
    """Create a new commit."""
    message: str = Field(..., min_length=1, max_length=500)
    parent_commit_id: Optional[str] = None
    table_name: Optional[str] = Field("primary", description="Name of the table to create/update")
    data: List[Dict[str, Any]] = Field(..., description="List of data rows to include in the commit")


class CreateBranchRequest(BaseModel):
    """Create a new branch/ref."""
    ref_name: str = Field(..., min_length=1, max_length=100)
    commit_id: str = Field(..., min_length=1)
    
    @field_validator('ref_name')
    @classmethod
    def validate_ref_name(cls, v: str) -> str:
        """Validate ref name format."""
        if not v.replace('-', '').replace('_', '').replace('/', '').isalnum():
            raise ValueError("Ref name must contain only alphanumeric characters, hyphens, underscores, and slashes")
        return v


# ============================================
# Data Access Request Models
# ============================================

# ============================================
# Enhanced Data Query Request Models (POST)
# ============================================

class PaginationParams(BaseModel):
    """Pagination parameters for data queries."""
    cursor: Optional[str] = Field(None, description="Cursor for infinite scroll")
    offset: Optional[int] = Field(0, ge=0, description="Offset for standard pagination")
    limit: int = Field(100, ge=1, le=10000, description="Number of rows to return")


class SortSpec(BaseModel):
    """Single sort specification."""
    column: str = Field(..., description="Column name to sort by")
    desc: bool = Field(False, description="Sort in descending order")


class FilterOperator(str):
    """Valid filter operators."""
    EQ = "eq"
    NEQ = "neq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


class ColumnFilter(BaseModel):
    """Single column filter specification."""
    column: str = Field(..., description="Column name to filter")
    operator: Literal["eq", "neq", "gt", "gte", "lt", "lte", "contains", "not_contains", "starts_with", "ends_with", "in", "not_in", "is_null", "is_not_null"] = Field(..., description="Filter operator")
    value: Optional[Any] = Field(None, description="Filter value (not required for is_null/is_not_null)")
    
    @field_validator('value')
    @classmethod
    def validate_value(cls, v: Any, info) -> Any:
        """Validate value based on operator."""
        # In Pydantic V2, the validation context is passed as ValidationInfo
        operator = info.data.get('operator') if hasattr(info, 'data') else None
        if operator in ['is_null', 'is_not_null'] and v is not None:
            raise ValueError(f"Operator {operator} should not have a value")
        if operator not in ['is_null', 'is_not_null'] and v is None:
            raise ValueError(f"Operator {operator} requires a value")
        if operator in ['in', 'not_in'] and not isinstance(v, list):
            raise ValueError(f"Operator {operator} requires a list value")
        return v


class FilterGroup(BaseModel):
    """Group of filters with logic operator."""
    logic: Literal["AND", "OR"] = Field("AND", description="Logic operator for combining filters")
    conditions: List[ColumnFilter] = Field(..., min_length=1, description="Filter conditions")


class DataFilters(BaseModel):
    """Complete filter specification."""
    global_filter: Optional[str] = Field(None, description="Global text search")
    columns: Optional[List[ColumnFilter]] = Field(None, description="Column filters (AND logic)")
    groups: Optional[List[FilterGroup]] = Field(None, description="Filter groups for complex logic")


class DataQueryRequest(BaseModel):
    """Enhanced data query request for POST endpoint."""
    pagination: Optional[PaginationParams] = Field(default_factory=PaginationParams)
    sorting: Optional[List[SortSpec]] = Field(None, description="Multi-column sorting")
    filters: Optional[DataFilters] = Field(None, description="Data filters")
    select_columns: Optional[List[str]] = Field(None, description="Columns to return")
    format: Literal["nested", "flat"] = Field("nested", description="Response format")


# ============================================
# Permission Request Models
# ============================================

class GrantPermissionRequest(BaseModel):
    """Request to grant permission."""
    user_id: int = Field(..., ge=1)
    resource_type: str = Field(..., min_length=1)
    resource_id: int = Field(..., ge=1)
    permission_type: str = Field(..., min_length=1)


class RevokePermissionRequest(BaseModel):
    """Request to revoke permission."""
    user_id: int = Field(..., ge=1)
    resource_type: str = Field(..., min_length=1)
    resource_id: int = Field(..., ge=1)


# ============================================
# Sampling Request Models
# ============================================

class CreateSamplingRequest(BaseModel):
    """Request to create a sampling configuration."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    sampling_type: str = Field(..., description="Type of sampling: random, stratified, systematic")
    sample_size: Optional[int] = Field(None, ge=1, description="Number of samples")
    sample_percentage: Optional[float] = Field(None, gt=0, le=100, description="Percentage of data to sample")
    stratify_columns: Optional[List[str]] = Field(None, description="Columns for stratified sampling")
    random_seed: Optional[int] = Field(None, description="Random seed for reproducibility")
    
    @field_validator('sample_size')
    @classmethod
    def validate_sampling_params(cls, v: Optional[int], info) -> Optional[int]:
        """Ensure either sample_size or sample_percentage is provided."""
        sample_percentage = info.data.get('sample_percentage') if hasattr(info, 'data') else None
        if v is None and sample_percentage is None:
            raise ValueError("Either sample_size or sample_percentage must be provided")
        return v


class RunSamplingRequest(BaseModel):
    """Request to run a sampling job."""
    configuration_id: int = Field(..., ge=1)
    commit_id: Optional[str] = Field(None, description="Specific commit to sample from")
    output_branch: Optional[str] = Field(None, description="Branch to save results to")


# ============================================
# Exploration Request Models
# ============================================

class ExplorationRequest(BaseModel):
    """Request for data exploration."""
    analysis_type: str = Field(..., description="Type of analysis: overview, distribution, correlation")
    columns: Optional[List[str]] = Field(None, description="Specific columns to analyze")
    include_statistics: bool = Field(True, description="Include statistical measures")
    include_visualizations: bool = Field(False, description="Include visualization data")


# ============================================
# SQL Transform Request Models
# ============================================

class SQLTransformRequest(BaseModel):
    """Request to run SQL transformation."""
    sql_query: str = Field(..., min_length=1, description="SQL query to execute")
    source_tables: Dict[str, str] = Field(..., description="Mapping of table aliases to dataset refs")
    output_table: str = Field(..., description="Name for the output table")
    output_branch: Optional[str] = Field(None, description="Branch to save results to")
    
    @field_validator('sql_query')
    @classmethod
    def validate_sql(cls, v: str) -> str:
        """Basic SQL validation."""
        # Prevent dangerous operations
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
        sql_upper = v.upper()
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                raise ValueError(f"SQL contains forbidden keyword: {keyword}")
        return v