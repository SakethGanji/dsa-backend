from pydantic import BaseModel, constr, Field
from datetime import datetime
from typing import Optional
from enum import Enum

class UserCreate(BaseModel):
    soeid: constr(min_length=7, max_length=7)
    password: str  # Changed from password_hash
    role_id: int

class UserOut(BaseModel):
    id: int
    soeid: str
    role_id: int
    role_name: Optional[str] = None
    created_at: datetime
    updated_at: datetime

# Permission types match the ENUMs in the schema
class DatasetPermissionType(str, Enum):
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"

class FilePermissionType(str, Enum):
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"

# Dataset permissions
class DatasetPermissionBase(BaseModel):
    dataset_id: int
    user_id: int
    permission_type: DatasetPermissionType

class DatasetPermissionCreate(DatasetPermissionBase):
    pass

class DatasetPermission(DatasetPermissionBase):
    class Config:
        from_attributes = True

# File permissions
class FilePermissionBase(BaseModel):
    file_id: int
    user_id: int
    permission_type: FilePermissionType

class FilePermissionCreate(FilePermissionBase):
    pass

class FilePermission(FilePermissionBase):
    class Config:
        from_attributes = True

# Generic permission grant request (for API)
class PermissionGrant(BaseModel):
    user_id: int = Field(..., description="User ID to grant permission to")
    permission_type: str = Field(..., description="Permission type to grant")

# Backwards compatibility aliases
PermissionType = DatasetPermissionType
