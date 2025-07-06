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


class CreateDatasetResponse(BaseModel):
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = []


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