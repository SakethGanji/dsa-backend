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
    created_at: datetime
    updated_at: datetime

class PermissionType(str, Enum):
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"

class ResourceType(str, Enum):
    DATASET = "dataset"
    FILE = "file"

class PermissionBase(BaseModel):
    resource_type: ResourceType
    resource_id: int
    user_id: int
    permission_type: PermissionType

class PermissionCreate(PermissionBase):
    granted_by: int

class Permission(PermissionBase):
    id: int
    granted_at: datetime
    granted_by: int

    class Config:
        from_attributes = True

class PermissionGrant(BaseModel):
    user_id: int = Field(..., description="User ID to grant permission to")
    permission_type: PermissionType = Field(..., description="Permission type to grant")
