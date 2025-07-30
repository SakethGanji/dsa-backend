"""Common API types and models."""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum


# ============================================
# Enums
# ============================================

class PermissionType(str, Enum):
    """Permission types for datasets."""
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"
    OWNER = "owner"


# ============================================
# Authentication Models
# ============================================

class CurrentUser(BaseModel):
    """Current authenticated user information."""
    soeid: str
    user_id: int
    role_id: int
    role_name: Optional[str] = None
    
    def is_admin(self) -> bool:
        """Check if user has admin role."""
        return self.role_id == 1
    
    def is_manager(self) -> bool:
        """Check if user has manager role."""
        return self.role_id == 2
    
    def is_analyst(self) -> bool:
        """Check if user has analyst role."""
        return self.role_id == 3
    
    def is_viewer(self) -> bool:
        """Check if user has viewer role."""
        return self.role_id == 4


# ============================================
# Summary Models (for list responses)
# ============================================

class DatasetSummary(BaseModel):
    """Summary information for a dataset in list views."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    created_by: int
    created_at: datetime
    updated_at: datetime
    permission_type: Optional[str] = None  # User's permission on this dataset
    tags: List[str] = Field(default_factory=list)
    import_status: Optional[str] = None
    import_job_id: Optional[str] = None  # UUID as string


class UserSummary(BaseModel):
    """Summary information for a user in list views."""
    user_id: int
    soeid: str
    role_id: int
    role_name: Optional[str] = None
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime] = None


class JobSummary(BaseModel):
    """Summary information for a job in list views."""
    job_id: str  # UUID as string
    run_type: str
    status: str
    dataset_id: Optional[int] = None
    dataset_name: Optional[str] = None
    user_id: Optional[int] = None
    user_soeid: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None


class DatasetListItem(BaseModel):
    """Simple dataset item for search results."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    created_by: int
    permission_type: Optional[str] = None


# ============================================
# Domain Models (used in responses)
# ============================================

class DataRow(BaseModel):
    """Single row of data."""
    sheet_name: str
    logical_row_id: str
    data: Dict[str, Any]


class ColumnSchema(BaseModel):
    """Column schema information."""
    name: str
    type: str
    nullable: bool = True


class SheetSchema(BaseModel):
    """Sheet schema information."""
    sheet_name: str
    columns: List[ColumnSchema]
    row_count: int


class JobDetail(BaseModel):
    """Detailed job information."""
    job_id: str  # UUID as string
    run_type: str
    status: str
    dataset_id: Optional[int] = None
    dataset_name: Optional[str] = None
    source_commit_id: Optional[str] = None
    user_id: Optional[int] = None
    user_soeid: Optional[str] = None
    run_parameters: Optional[Dict[str, Any]] = None
    output_summary: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None


class CommitInfo(BaseModel):
    """Commit information."""
    commit_id: str
    parent_commit_id: Optional[str] = None
    message: str
    author_id: int
    author_soeid: str
    created_at: datetime
    table_count: int = 0
    is_head: bool = False


class RefInfo(BaseModel):
    """Reference/branch information."""
    ref_name: str
    commit_id: str
    dataset_id: int
    is_default: bool = False
    created_at: datetime
    updated_at: datetime


class TableInfo(BaseModel):
    """Table information."""
    table_key: str
    sheet_name: str
    row_count: int
    column_count: int
    created_at: datetime
    commit_id: str


class RefWithTables(BaseModel):
    """Reference with associated tables."""
    ref_name: str
    commit_id: str
    is_default: bool
    created_at: datetime
    updated_at: datetime
    tables: List[TableInfo]


class SearchResult(BaseModel):
    """Individual search result."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    created_by: int
    created_by_soeid: str
    rank: Optional[float] = None  # Search relevance score