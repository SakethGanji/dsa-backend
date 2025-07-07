from pydantic import BaseModel, Field, constr
from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID
from enum import Enum


# Dataset models
class CreateDatasetRequest(BaseModel):
    name: str
    description: Optional[str] = None
    tags: Optional[List[str]] = Field(default=[], description="List of tags for the dataset")
    default_branch: str = Field(default="main", description="Default branch name for the dataset")


class CreateDatasetResponse(BaseModel):
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = []


# Combined create dataset and import models
class CreateDatasetWithFileRequest(BaseModel):
    name: str
    description: Optional[str] = None
    tags: Optional[List[str]] = Field(default=[], description="List of tags for the dataset")
    default_branch: str = Field(default="main", description="Default branch name for the dataset")
    commit_message: str = Field(default="Initial import", description="Message describing the import")


class CreateDatasetWithFileResponse(BaseModel):
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = []
    import_job_id: UUID
    status: str = "pending"
    message: str = "Dataset created and import job queued successfully"


# File upload models
class QueueImportRequest(BaseModel):
    commit_message: str = Field(..., description="Message describing the import")


class QueueImportResponse(BaseModel):
    job_id: UUID
    status: str = "pending"
    message: str = "Import job queued successfully"


# Commit creation models
class CreateCommitRequest(BaseModel):
    parent_commit_id: Optional[str] = None
    message: str
    data: List[Dict[str, Any]]  # For direct data commits (not file imports)


class CreateCommitResponse(BaseModel):
    commit_id: str
    dataset_id: int
    message: str


# Data retrieval models
class GetDataRequest(BaseModel):
    table_key: Optional[str] = None
    offset: int = 0
    limit: int = 100


class DataRow(BaseModel):
    logical_row_id: str
    data: Dict[str, Any]


class GetDataResponse(BaseModel):
    dataset_id: int
    ref_name: str
    commit_id: str
    rows: List[DataRow]
    total_rows: int
    offset: int
    limit: int


# Schema models
class ColumnSchema(BaseModel):
    name: str
    type: str
    nullable: bool = True


class SheetSchema(BaseModel):
    sheet_name: str
    columns: List[ColumnSchema]
    row_count: int


class CommitSchemaResponse(BaseModel):
    commit_id: str
    sheets: List[SheetSchema]


# Table analysis models
class TableAnalysisResponse(BaseModel):
    table_key: str
    columns: List[str]
    column_types: Dict[str, str]
    total_rows: int
    null_counts: Dict[str, int]
    sample_values: Dict[str, List[Any]]
    statistics: Optional[Dict[str, Any]] = None


# Job status models
class JobStatusResponse(BaseModel):
    job_id: UUID
    run_type: str
    status: str
    dataset_id: int
    created_at: datetime
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    output_summary: Optional[Dict[str, Any]] = None


# User models
class CreateUserRequest(BaseModel):
    soeid: constr(min_length=7, max_length=7)
    password: str
    role_id: int


class CreateUserResponse(BaseModel):
    id: int
    soeid: str
    role_id: int
    role_name: Optional[str] = None
    created_at: datetime


class LoginRequest(BaseModel):
    soeid: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str
    user_id: int
    soeid: str
    role_id: int
    role_name: Optional[str] = None


class CurrentUser(BaseModel):
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


# Permission models
class PermissionType(str, Enum):
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


class GrantPermissionRequest(BaseModel):
    user_id: int
    permission_type: PermissionType


class GrantPermissionResponse(BaseModel):
    dataset_id: int
    user_id: int
    permission_type: str
    message: str = "Permission granted successfully"


# Dataset listing models
class DatasetSummary(BaseModel):
    dataset_id: int
    name: str
    description: Optional[str] = None
    created_by: int
    created_at: datetime
    updated_at: datetime
    permission_type: str
    tags: List[str] = []


class ListDatasetsResponse(BaseModel):
    datasets: List[DatasetSummary]
    total: int
    offset: int
    limit: int


# Dataset detail models
class DatasetDetailResponse(BaseModel):
    dataset_id: int
    name: str
    description: Optional[str] = None
    created_by: int
    created_at: datetime
    updated_at: datetime
    tags: List[str] = []
    permission_type: Optional[str] = None


# Dataset update models
class UpdateDatasetRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None


class UpdateDatasetResponse(BaseModel):
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = []
    message: str = "Dataset updated successfully"


# Dataset delete models
class DeleteDatasetResponse(BaseModel):
    dataset_id: int
    message: str = "Dataset deleted successfully"


# Commit History Models
class CommitInfo(BaseModel):
    commit_id: str
    parent_commit_id: Optional[str]
    message: str
    author_id: int
    author_name: str  # Enriched field
    created_at: datetime
    row_count: int    # Number of rows in this commit


class GetCommitHistoryResponse(BaseModel):
    commits: List[CommitInfo]
    total: int
    offset: int
    limit: int
    has_more: bool = False


# Checkout Models
class CheckoutResponse(BaseModel):
    commit_id: str
    data: List[Dict[str, Any]]
    total_rows: int
    offset: int
    limit: int
    has_more: bool = False


# Job Models
class JobSummary(BaseModel):
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


class JobDetail(BaseModel):
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
    created_at: Optional[str]
    completed_at: Optional[str]
    duration_seconds: Optional[float]


class JobListResponse(BaseModel):
    jobs: List[JobSummary]
    total: int
    offset: int
    limit: int


class JobDetailResponse(BaseModel):
    job: JobDetail


# Branch/Ref models
class RefInfo(BaseModel):
    """Information about a ref/branch"""
    name: str
    commit_id: Optional[str]
    created_at: datetime
    updated_at: datetime


class ListRefsResponse(BaseModel):
    """Response for listing all refs/branches"""
    refs: List[RefInfo]
    dataset_id: int


class CreateBranchRequest(BaseModel):
    """Request to create a new branch"""
    name: constr(pattern=r'^[a-zA-Z0-9][a-zA-Z0-9-_]*$', min_length=1, max_length=255) = Field(
        ..., 
        description="Branch name (alphanumeric, hyphens, underscores allowed)"
    )
    from_ref: str = Field(
        default="main",
        description="Source ref to branch from"
    )


class CreateBranchResponse(BaseModel):
    """Response after creating a branch"""
    name: str
    commit_id: str
    created_from: str
    message: str = "Branch created successfully"


# Dataset Overview Models
class TableInfo(BaseModel):
    """Basic information about a table"""
    table_key: str
    row_count: Optional[int] = None
    column_count: Optional[int] = None


class RefWithTables(BaseModel):
    """A ref/branch with its associated tables"""
    ref_name: str
    commit_id: Optional[str]
    tables: List[TableInfo]
    created_at: datetime
    updated_at: datetime


class DatasetOverviewResponse(BaseModel):
    """Overview of a dataset including all refs and their tables"""
    dataset_id: int
    dataset_name: str
    refs: List[RefWithTables]
    default_ref: str = "main"