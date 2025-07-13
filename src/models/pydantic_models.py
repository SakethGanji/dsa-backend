"""Refactored Pydantic models using base model inheritance."""

from pydantic import BaseModel, Field, constr
from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID
from enum import Enum

# Import base models
from .base_models import (
    BaseDatasetModel, BaseUserModel, BaseJobModel,
    BaseDeleteResponse, BasePaginatedResponse,
    BaseOperationResponse, BaseDetailResponse,
    BaseNamedEntityModel, BasePermissionModel,
    PermissionLevel, JobStatus, ImportStatus,
    BaseCommitModel, BaseBranchModel
)


# ============================================
# Dataset Models - Using Inheritance
# ============================================

class CreateDatasetRequest(BaseModel):
    """Request to create a new dataset."""
    name: str
    description: Optional[str] = None
    tags: Optional[List[str]] = Field(default=[], description="List of tags for the dataset")
    default_branch: str = Field(default="main", description="Default branch name for the dataset")


class CreateDatasetResponse(BaseModel):
    """Response after creating a dataset."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = []


class CreateDatasetWithFileRequest(CreateDatasetRequest):
    """Request to create dataset with file upload."""
    commit_message: str = Field(default="Initial import", description="Message describing the import")


class CreateDatasetWithFileResponse(BaseDatasetModel):
    """Response after creating dataset with file."""
    # Inherits: dataset_id (as id), name, description, tags, created_at, updated_at
    dataset_id: int
    import_job_id: UUID
    status: str = ImportStatus.PENDING
    message: str = "Dataset created and import job queued successfully"
    
    class Config:
        populate_by_name = False  # Don't use alias


# ============================================
# Dataset Summary & Details - Using Inheritance
# ============================================

class DatasetSummary(BaseModel):
    """Summary of a dataset for listing."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    created_by: int
    created_at: datetime
    updated_at: datetime
    permission_type: str
    tags: List[str] = []
    import_status: Optional[str] = None
    import_job_id: Optional[str] = None  # UUID as string


class DatasetDetailResponse(BaseDatasetModel):
    """Detailed dataset information."""
    # Inherits: dataset_id (as id), name, description, tags, created_at, updated_at
    dataset_id: int
    created_by: int
    permission_type: Optional[str] = None
    import_status: Optional[str] = None
    import_job_id: Optional[str] = None  # UUID as string
    
    class Config:
        populate_by_name = False  # Don't use alias


class ListDatasetsResponse(BaseModel):
    """Paginated list of datasets."""
    datasets: List[DatasetSummary]
    total: int
    offset: int
    limit: int


# ============================================
# Dataset Update & Delete - Using Inheritance
# ============================================

class UpdateDatasetRequest(BaseModel):
    """Request to update dataset."""
    name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None


class UpdateDatasetResponse(BaseDatasetModel):
    """Response after updating dataset."""
    # Inherits: dataset_id (as id), name, description, tags, created_at, updated_at
    dataset_id: int
    metadata: Optional[Dict[str, Any]] = None
    
    class Config:
        populate_by_name = False  # Don't use alias


class DeleteDatasetResponse(BaseDeleteResponse):
    """Response after deleting dataset."""
    # Inherits: success, message, entity_type, entity_id
    # Will be initialized with entity_type="Dataset"
    pass


# ============================================
# User Models - Using Inheritance
# ============================================

class CreateUserRequest(BaseModel):
    """Request to create a new user."""
    soeid: str
    password: str
    role_id: Optional[int] = None


class CreateUserResponse(BaseUserModel):
    """Response after creating a user."""
    # Inherits: user_id (as id), soeid, role_id, role_name, created_at, updated_at
    user_id: int
    message: str = "User created successfully"
    
    class Config:
        populate_by_name = False  # Don't use alias


class UpdateUserRequest(BaseModel):
    """Request to update user."""
    soeid: Optional[str] = None
    password: Optional[str] = None
    role_id: Optional[int] = None


class UpdateUserResponse(BaseUserModel):
    """Response after updating user."""
    # Inherits: user_id (as id), soeid, role_id, role_name, created_at, updated_at
    user_id: int
    message: str = "User updated successfully"
    
    class Config:
        populate_by_name = False  # Don't use alias


class DeleteUserResponse(BaseDeleteResponse):
    """Response after deleting user."""
    # Inherits: success, message, entity_type, entity_id
    # Will be initialized with entity_type="User"
    pass


class UserSummary(BaseUserModel):
    """Summary of a user for listing."""
    # Inherits: user_id (as id), soeid, role_id, role_name, created_at, updated_at
    user_id: int
    
    class Config:
        populate_by_name = False  # Don't use alias


class ListUsersResponse(BasePaginatedResponse[UserSummary]):
    """Paginated list of users."""
    # Need to redefine for compatibility
    users: List[UserSummary] = Field(alias="items")
    total: int
    offset: int
    limit: int
    
    class Config:
        populate_by_name = True


# ============================================
# File Upload & Import Models
# ============================================

class QueueImportRequest(BaseModel):
    """Request to queue an import job."""
    commit_message: str = Field(..., description="Message describing the import")


class QueueImportResponse(BaseModel):
    """Response after queuing import job."""
    job_id: UUID
    status: str = "pending"
    message: str = "Import job queued successfully"


# ============================================
# Job Models
# ============================================

class JobSummary(BaseModel):
    """Summary of a job for listing."""
    id: str
    run_type: str
    status: str
    dataset_id: Optional[int]
    dataset_name: Optional[str]
    user_id: Optional[int]
    user_soeid: Optional[str]
    created_at: Optional[str]
    completed_at: Optional[str]
    error_message: Optional[str]
    progress: Optional[float]


class JobDetail(BaseModel):
    """Detailed job information."""
    id: str
    run_type: str
    status: str
    dataset_id: Optional[int]
    dataset_name: Optional[str]
    source_commit_id: Optional[str]
    user_id: Optional[int]
    user_soeid: Optional[str]
    run_parameters: Optional[Dict[str, Any]]
    output_summary: Optional[Dict[str, Any]]
    error_message: Optional[str]
    created_at: datetime
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]


class JobListResponse(BaseModel):
    """Response for job listing."""
    jobs: List[JobSummary]
    total: int
    offset: int
    limit: int


class JobDetailResponse(BaseModel):
    """Response for job details."""
    job: JobDetail


class CancelJobRequest(BaseModel):
    """Request to cancel a job."""
    reason: Optional[str] = None


class CancelJobResponse(BaseModel):
    """Response after cancelling a job."""
    job_id: str
    previous_status: str
    new_status: str = "cancelled"
    message: str = "Job cancelled successfully"


# ============================================
# Commit & Version Models
# ============================================

class CreateCommitRequest(BaseModel):
    """Request to create a commit."""
    parent_commit_id: Optional[str] = None
    message: str
    data: List[Dict[str, Any]]  # For direct data commits


class CreateCommitResponse(BaseModel):
    """Response after creating commit."""
    commit_id: str
    dataset_id: int
    message: str


# ============================================
# Data Access Models
# ============================================

class GetDataRequest(BaseModel):
    """Request to get data from dataset."""
    table_key: Optional[str] = None
    offset: int = 0
    limit: int = 100


class DataRow(BaseModel):
    """Single data row."""
    logical_row_id: str
    data: Dict[str, Any]


class GetDataResponse(BaseModel):
    """Response with dataset data."""
    dataset_id: int
    ref_name: str
    commit_id: str
    rows: List[DataRow]
    total_rows: int
    offset: int
    limit: int


# ============================================
# Schema Models
# ============================================

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
    columns: List[str]
    column_types: Dict[str, str]
    total_rows: int
    null_counts: Dict[str, int]
    sample_values: Optional[Dict[str, List[Any]]] = None
    statistics: Optional[Dict[str, Any]] = None


# ============================================
# Commit History & Checkout Models
# ============================================

class CommitInfo(BaseModel):
    """Information about a commit."""
    commit_id: str
    parent_commit_id: Optional[str] = None
    message: str
    author_id: int
    author_name: Optional[str] = None
    created_at: datetime
    sheets: Optional[List[str]] = None


class GetCommitHistoryResponse(BaseModel):
    """Response for commit history."""
    commits: List[CommitInfo]
    total: int
    offset: int
    limit: int
    has_more: bool = False


class CheckoutResponse(BaseModel):
    """Response for checkout operation."""
    commit_id: str
    data: List[Dict[str, Any]]
    total_rows: int
    offset: int
    limit: int


# ============================================
# Ref/Branch Models
# ============================================

class RefInfo(BaseModel):
    """Information about a ref/branch."""
    ref_name: str
    commit_id: str
    is_default: bool = False
    created_at: datetime
    updated_at: datetime


class CreateBranchRequest(BaseModel):
    """Request to create a new branch."""
    name: constr(pattern=r'^[a-zA-Z0-9][a-zA-Z0-9-_]*$', min_length=1, max_length=255) = Field(
        ..., 
        description="Branch name (alphanumeric, hyphens, underscores allowed)"
    )
    from_ref: str = Field(
        "main",
        description="Reference (branch/commit) to create from"
    )


class CreateBranchResponse(BaseModel):
    """Response after creating a branch."""
    ref_name: str
    commit_id: str
    dataset_id: int
    message: str = "Branch created successfully"


class ListRefsResponse(BaseModel):
    """Response for listing all refs/branches."""
    refs: List[RefInfo]
    dataset_id: int


class TableInfo(BaseModel):
    """Information about a table."""
    table_key: str
    row_count: int
    column_count: int
    columns: List[str]
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RefWithTables(BaseModel):
    """Ref with its tables."""
    ref_name: str
    commit_id: str
    is_default: bool = False
    tables: List[TableInfo]


class DatasetOverviewResponse(BaseModel):
    """Overview of a dataset including all refs and their tables."""
    dataset_id: int
    dataset_name: str
    refs: List[RefWithTables]
    default_ref: str = "main"


# ============================================
# Permission Models
# ============================================

class PermissionType(str, Enum):
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


class GrantPermissionRequest(BaseModel):
    """Request to grant permission."""
    user_id: int
    permission_type: PermissionType


class GrantPermissionResponse(BaseModel):
    """Response after granting permission."""
    dataset_id: int
    user_id: int
    permission_type: str
    message: str = "Permission granted successfully"


# ============================================
# Authentication Models (No Changes Needed)
# ============================================

class LoginRequest(BaseModel):
    """Login request."""
    soeid: str
    password: str


class LoginResponse(BaseModel):
    """Login response with tokens."""
    access_token: str
    refresh_token: str
    token_type: str
    user_id: int
    soeid: str
    role_id: int
    role_name: Optional[str] = None


class CurrentUser(BaseModel):
    """Current user information."""
    soeid: str
    user_id: int
    role_id: int
    role_name: Optional[str] = None
    
    def is_admin(self) -> bool:
        return self.role_name == 'admin' or self.role_id == 1
    
    def is_manager(self) -> bool:
        return self.role_id == 2
    
    def can_edit_datasets(self) -> bool:
        return self.is_admin() or self.is_manager()


# ============================================
# Additional Models (Compatibility)
# ============================================

class DatasetListItem(BaseModel):
    """Dataset list item for simple listings."""
    id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = []
    created_at: datetime
    updated_at: datetime


# ============================================
# Search Models
# ============================================

class SearchResult(BaseModel):
    """Search result item."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = []
    created_at: datetime
    updated_at: datetime
    permission_type: str
    score: Optional[float] = None


class SearchResponse(BaseModel):
    """Search response."""
    results: List[SearchResult]
    total: int
    offset: int
    limit: int
    query: str
    facets: Optional[Dict[str, List[Dict[str, Any]]]] = None