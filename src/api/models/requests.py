"""API Request models."""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any
from uuid import UUID


# ============================================
# Dataset Request Models
# ============================================

class CreateDatasetRequest(BaseModel):
    """Create a new dataset."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    tags: List[str] = Field(default_factory=list, max_items=20)
    default_branch: str = Field("main", min_length=1, max_length=100)


class CreateDatasetWithFileRequest(CreateDatasetRequest):
    """Create dataset with file import in one operation."""
    default_branch: str = Field("main")
    commit_message: str = Field("Initial import")


class UpdateDatasetRequest(BaseModel):
    """Update dataset metadata."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    tags: Optional[List[str]] = Field(None, max_items=20)


# ============================================
# User Request Models
# ============================================

class CreateUserRequest(BaseModel):
    """Create a new user."""
    soeid: str = Field(..., min_length=1, max_length=50)
    password: str = Field(..., min_length=8)
    role_id: int = Field(..., ge=1, le=4)
    is_active: bool = Field(True)


class UpdateUserRequest(BaseModel):
    """Update user information."""
    password: Optional[str] = Field(None, min_length=8)
    role_id: Optional[int] = Field(None, ge=1, le=4)
    is_active: Optional[bool] = None


class LoginRequest(BaseModel):
    """User login credentials."""
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

class GetDataRequest(BaseModel):
    """Request dataset data with pagination."""
    offset: int = Field(0, ge=0)
    limit: int = Field(100, ge=1, le=10000)
    sheet_name: Optional[str] = None


# ============================================
# Permission Request Models
# ============================================

class GrantPermissionRequest(BaseModel):
    """Grant permission to a user."""
    user_id: int
    permission_type: str = Field(..., pattern="^(read|write|admin)$")