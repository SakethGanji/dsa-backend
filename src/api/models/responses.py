"""API Response models."""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import UUID

# Import types from common to avoid forward references
from .common import (
    DatasetSummary, UserSummary, JobSummary, JobDetail,
    DataRow, SheetSchema, CommitInfo, RefInfo,
    TableInfo, RefWithTables, SearchResult
)


# ============================================
# Dataset Response Models
# ============================================

class CreateDatasetResponse(BaseModel):
    """Response after creating a dataset."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str]
    created_at: datetime


class CreateDatasetWithFileResponse(BaseModel):
    """Response after creating dataset with file."""
    dataset: CreateDatasetResponse
    commit_id: str
    import_job: 'QueueImportResponse'  # Keep as forward ref - defined later in file
    
    class Config:
        """Allow forward references."""
        arbitrary_types_allowed = True


class DatasetDetailResponse(BaseModel):
    """Detailed dataset information."""
    id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    created_by: int
    permission_type: Optional[str] = None
    import_status: Optional[str] = None
    import_job_id: Optional[str] = None  # UUID as string


class ListDatasetsResponse(BaseModel):
    """Paginated list of datasets."""
    datasets: List[DatasetSummary]
    total: int
    offset: int
    limit: int


class UpdateDatasetResponse(BaseModel):
    """Response after updating a dataset."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str]
    updated_at: datetime


class DeleteDatasetResponse(BaseModel):
    """Response after deleting a dataset."""
    success: bool = True
    message: str = "Dataset deleted successfully"


class DatasetOverviewResponse(BaseModel):
    """Complete dataset overview with all branches."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    branches: List[RefWithTables]


# ============================================
# User Response Models
# ============================================

class CreateUserResponse(BaseModel):
    """Response after creating a user."""
    user_id: int
    soeid: str
    role_id: int
    role_name: Optional[str] = None
    is_active: bool
    created_at: datetime


class UpdateUserResponse(BaseModel):
    """Response after updating a user."""
    user_id: int
    soeid: str
    role_id: int
    role_name: Optional[str] = None
    is_active: bool
    updated_at: datetime


class DeleteUserResponse(BaseModel):
    """Response after deleting a user."""
    success: bool = True
    message: str = "User deleted successfully"


class ListUsersResponse(BaseModel):
    """Paginated list of users."""
    users: List[UserSummary]
    total: int
    offset: int
    limit: int
    
    class Config:
        """Configuration for the model."""
        json_schema_extra = {
            "example": {
                "users": [{"user_id": 1, "soeid": "ab12345", "role_id": 2, "role_name": "analyst"}],
                "total": 1,
                "offset": 0,
                "limit": 10
            }
        }


class LoginResponse(BaseModel):
    """Response after successful login."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user_id: int
    soeid: str
    role_id: int
    role_name: Optional[str] = None


# ============================================
# Job Response Models
# ============================================

class QueueImportResponse(BaseModel):
    """Response after queuing an import job."""
    job_id: str  # UUID as string
    status: str = "pending"
    message: str = "Import job queued successfully"


class JobListResponse(BaseModel):
    """Paginated list of jobs."""
    jobs: List[JobSummary]
    total: int
    offset: int
    limit: int


class JobDetailResponse(BaseModel):
    """Detailed job information."""
    job: JobDetail


class CancelJobResponse(BaseModel):
    """Response after cancelling a job."""
    job_id: str  # UUID as string
    status: str = "cancelled"
    message: str = "Job cancelled successfully"
    cancelled_at: datetime


# ============================================
# Version Control Response Models
# ============================================

class CreateCommitResponse(BaseModel):
    """Response after creating a commit."""
    commit_id: str
    message: str
    created_at: datetime


class GetCommitHistoryResponse(BaseModel):
    """Commit history for a dataset."""
    dataset_id: int
    ref_name: str
    commits: List[CommitInfo]
    total: int
    offset: int
    limit: int


class CheckoutResponse(BaseModel):
    """Response after checking out a ref."""
    dataset_id: int
    ref_name: str
    commit_id: str
    message: str = "Checkout successful"
    tables: List[TableInfo]


class CreateBranchResponse(BaseModel):
    """Response after creating a branch."""
    dataset_id: int
    ref_name: str
    commit_id: str
    created_at: datetime


class ListRefsResponse(BaseModel):
    """List of refs for a dataset."""
    refs: List[RefInfo]
    default_branch: str = "main"


# ============================================
# Data Access Response Models
# ============================================

class GetDataResponse(BaseModel):
    """Response with dataset data."""
    dataset_id: int
    ref_name: str
    commit_id: str
    rows: List[DataRow]
    total_rows: int
    offset: int
    limit: int


class GetSchemaResponse(BaseModel):
    """Response with schema information."""
    dataset_id: int
    ref_name: str
    commit_id: str
    sheets: List[SheetSchema]


class CommitSchemaResponse(BaseModel):
    """Schema for a specific commit."""
    commit_id: str
    sheets: List[SheetSchema]


class TableAnalysisResponse(BaseModel):
    """Table analysis information."""
    table_key: str
    sheet_name: str
    column_stats: Dict[str, Any]
    sample_data: List[Dict[str, Any]]
    row_count: int
    null_counts: Dict[str, int]
    unique_counts: Dict[str, int]
    data_types: Dict[str, str]
    columns: List[Dict[str, str]]  # List of {"name": "col_name", "type": "col_type"}


# ============================================
# Permission Response Models
# ============================================

class GrantPermissionResponse(BaseModel):
    """Response after granting permission."""
    dataset_id: int
    user_id: int
    permission_type: str
    granted_at: datetime
    message: str = "Permission granted successfully"


# ============================================
# Search Response Models
# ============================================

class SearchResponse(BaseModel):
    """Search results."""
    results: List[SearchResult]
    total: int
    offset: int
    limit: int
    query: str