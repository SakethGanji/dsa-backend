from pydantic import BaseModel, Field, constr
from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID
from enum import Enum


# Dataset models
class CreateDatasetRequest(BaseModel):
    name: str
    description: Optional[str] = None


class CreateDatasetResponse(BaseModel):
    dataset_id: int
    name: str


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
    sheet_name: Optional[str] = None
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