"""Enhanced validation models with comprehensive security and business rules."""

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic.types import constr, conint
from typing import Optional, List, Dict, Any
import re
from .validators import validate_no_sql_injection, validate_no_script_tags, validate_safe_filename


# ============================================
# Dataset Validation Models
# ============================================

class EnhancedCreateDatasetRequest(BaseModel):
    """Enhanced dataset creation with comprehensive validation."""
    name: constr(min_length=1, max_length=255, strip_whitespace=True) = Field(
        ..., 
        description="Dataset name (1-255 chars)",
        json_schema_extra={"example": "Sales_Data_2024"}
    )
    description: Optional[constr(max_length=1000, strip_whitespace=True)] = Field(
        None,
        description="Dataset description (max 1000 chars)",
        json_schema_extra={"example": "Q4 2024 sales data for North America region"}
    )
    tags: List[constr(min_length=1, max_length=50)] = Field(
        default_factory=list,
        max_length=20,
        description="Tags for categorization (max 20 tags, 50 chars each)",
        json_schema_extra={"example": ["sales", "2024", "north-america"]}
    )
    default_branch: constr(min_length=1, max_length=100) = Field(
        "main",
        description="Default branch name",
        json_schema_extra={"example": "main"}
    )
    
    @field_validator('name')
    @classmethod
    def validate_dataset_name(cls, v: str) -> str:
        """Validate dataset name format and security."""
        # Security validation
        v = validate_no_sql_injection(v)
        v = validate_no_script_tags(v)
        
        # Format validation
        if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9_\-\s]*$', v):
            raise ValueError("Dataset name must start with alphanumeric and contain only letters, numbers, spaces, underscores, and hyphens")
        
        return v
    
    @field_validator('description')
    @classmethod
    def validate_description(cls, v: Optional[str]) -> Optional[str]:
        """Validate description for security."""
        if v:
            v = validate_no_sql_injection(v)
            v = validate_no_script_tags(v)
        return v
    
    @field_validator('tags')
    @classmethod 
    def validate_tags(cls, v: List[str]) -> List[str]:
        """Validate each tag."""
        validated_tags = []
        for tag in v:
            tag = validate_no_sql_injection(tag)
            tag = validate_no_script_tags(tag)
            if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9_\-]*$', tag):
                raise ValueError(f"Tag '{tag}' must start with alphanumeric and contain only letters, numbers, underscores, and hyphens")
            validated_tags.append(tag.lower())
        
        # Remove duplicates while preserving order
        seen = set()
        unique_tags = []
        for tag in validated_tags:
            if tag not in seen:
                seen.add(tag)
                unique_tags.append(tag)
        
        return unique_tags


class EnhancedUpdateDatasetRequest(BaseModel):
    """Enhanced dataset update with validation."""
    name: Optional[constr(min_length=1, max_length=255, strip_whitespace=True)] = None
    description: Optional[constr(max_length=1000, strip_whitespace=True)] = None
    tags: Optional[List[constr(min_length=1, max_length=50)]] = Field(None, max_length=20)
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        """Validate name if provided."""
        if v:
            v = validate_no_sql_injection(v)
            v = validate_no_script_tags(v)
            if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9_\-\s]*$', v):
                raise ValueError("Dataset name must start with alphanumeric and contain only letters, numbers, spaces, underscores, and hyphens")
        return v
    
    @field_validator('description')
    @classmethod
    def validate_description(cls, v: Optional[str]) -> Optional[str]:
        """Validate description if provided."""
        if v:
            v = validate_no_sql_injection(v)
            v = validate_no_script_tags(v)
        return v
    
    @field_validator('tags')
    @classmethod
    def validate_tags(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate tags if provided."""
        if v is None:
            return v
        return EnhancedCreateDatasetRequest.validate_tags(v)


# ============================================
# Permission Validation Models
# ============================================

class EnhancedGrantPermissionRequest(BaseModel):
    """Enhanced permission grant with validation."""
    user_id: conint(gt=0) = Field(..., description="User ID to grant permission to")
    permission_type: str = Field(
        ...,
        description="Permission level: read, write, or admin",
        json_schema_extra={"example": "read"}
    )
    
    @field_validator('permission_type')
    @classmethod
    def validate_permission_type(cls, v: str) -> str:
        """Validate permission type."""
        v = v.lower().strip()
        if v not in ['read', 'write', 'admin']:
            raise ValueError("Permission type must be one of: read, write, admin")
        return v


# ============================================
# User Validation Models
# ============================================

class EnhancedCreateUserRequest(BaseModel):
    """Enhanced user creation with comprehensive validation."""
    soeid: constr(min_length=1, max_length=50, strip_whitespace=True) = Field(
        ...,
        description="User SOEID",
        json_schema_extra={"example": "ab12345"}
    )
    password: constr(min_length=8, max_length=128) = Field(
        ...,
        description="Password (min 8 chars, must include uppercase, lowercase, number)",
        json_schema_extra={"example": "SecurePass123!"}
    )
    role_id: conint(ge=1, le=4) = Field(
        ...,
        description="Role ID: 1=admin, 2=manager, 3=analyst, 4=viewer",
        json_schema_extra={"example": 3}
    )
    is_active: bool = Field(True, description="Whether user is active")
    
    @field_validator('soeid')
    @classmethod
    def validate_soeid(cls, v: str) -> str:
        """Validate SOEID format."""
        v = validate_no_sql_injection(v)
        if not re.match(r'^[a-zA-Z]{2}\d{5}$', v):
            raise ValueError("SOEID must be 2 letters followed by 5 digits (e.g., ab12345)")
        return v.lower()
    
    @field_validator('password')
    @classmethod
    def validate_password_strength(cls, v: str) -> str:
        """Validate password meets security requirements."""
        if not re.search(r'[A-Z]', v):
            raise ValueError("Password must contain at least one uppercase letter")
        if not re.search(r'[a-z]', v):
            raise ValueError("Password must contain at least one lowercase letter")
        if not re.search(r'\d', v):
            raise ValueError("Password must contain at least one number")
        return v


class EnhancedLoginRequest(BaseModel):
    """Enhanced login request with validation."""
    soeid: constr(min_length=1, max_length=50, strip_whitespace=True)
    password: constr(min_length=1, max_length=128)
    
    @field_validator('soeid')
    @classmethod
    def validate_soeid(cls, v: str) -> str:
        """Basic SOEID validation for login."""
        return validate_no_sql_injection(v).lower()


# ============================================
# Version Control Validation Models
# ============================================

class EnhancedCreateCommitRequest(BaseModel):
    """Enhanced commit creation with validation."""
    dataset_id: conint(gt=0) = Field(..., description="Dataset ID")
    message: constr(min_length=1, max_length=500, strip_whitespace=True) = Field(
        ...,
        description="Commit message",
        json_schema_extra={"example": "Updated Q4 sales figures"}
    )
    parent_commit_id: Optional[constr(min_length=1, max_length=64)] = Field(
        None,
        description="Parent commit ID (optional)",
        json_schema_extra={"example": "abc123def456"}
    )
    changes: Optional[Dict[str, Any]] = Field(
        None,
        description="Changes to commit (table updates, etc.)",
        json_schema_extra={
            "example": {
                "tables_added": ["sales_summary"],
                "tables_modified": ["sales_detail"],
                "tables_deleted": []
            }
        }
    )
    
    @field_validator('message')
    @classmethod
    def validate_message(cls, v: str) -> str:
        """Validate commit message."""
        v = validate_no_sql_injection(v)
        v = validate_no_script_tags(v)
        if len(v.strip()) < 3:
            raise ValueError("Commit message must be at least 3 characters long")
        return v
    
    @field_validator('parent_commit_id')
    @classmethod
    def validate_parent_commit(cls, v: Optional[str]) -> Optional[str]:
        """Validate parent commit ID format."""
        if v:
            if not re.match(r'^[a-fA-F0-9]+$', v):
                raise ValueError("Commit ID must be a hexadecimal string")
        return v
    
    @model_validator(mode='after')
    def validate_changes_structure(self) -> 'EnhancedCreateCommitRequest':
        """Validate changes structure if provided."""
        if self.changes:
            allowed_keys = {'tables_added', 'tables_modified', 'tables_deleted', 'metadata'}
            extra_keys = set(self.changes.keys()) - allowed_keys
            if extra_keys:
                raise ValueError(f"Unknown keys in changes: {extra_keys}")
        return self


# ============================================
# Data Access Validation Models
# ============================================

class EnhancedGetDataRequest(BaseModel):
    """Enhanced data retrieval with validation."""
    offset: conint(ge=0, le=1000000) = Field(
        0,
        description="Offset for pagination",
        json_schema_extra={"example": 0}
    )
    limit: conint(ge=1, le=10000) = Field(
        100,
        description="Number of rows to return (max 10000)",
        json_schema_extra={"example": 100}
    )
    sheet_name: Optional[constr(min_length=1, max_length=255)] = Field(
        None,
        description="Specific sheet to retrieve",
        json_schema_extra={"example": "Sheet1"}
    )
    
    @field_validator('sheet_name')
    @classmethod
    def validate_sheet_name(cls, v: Optional[str]) -> Optional[str]:
        """Validate sheet name."""
        if v:
            v = validate_no_sql_injection(v)
            v = validate_no_script_tags(v)
        return v


# ============================================
# Import Validation Models
# ============================================

class EnhancedQueueImportRequest(BaseModel):
    """Enhanced import request with file validation."""
    dataset_id: conint(gt=0) = Field(..., description="Dataset ID")
    file_path: constr(min_length=1, max_length=500) = Field(
        ...,
        description="Path to uploaded file",
        json_schema_extra={"example": "/tmp/uploads/data.csv"}
    )
    
    @field_validator('file_path')
    @classmethod
    def validate_file_path(cls, v: str) -> str:
        """Validate file path for safety."""
        # Extract filename from path
        filename = v.split('/')[-1]
        validate_safe_filename(filename)
        return v


# ============================================
# Utility Models
# ============================================

class PaginationParams(BaseModel):
    """Standard pagination parameters."""
    offset: conint(ge=0, le=1000000) = Field(0, description="Offset for pagination")
    limit: conint(ge=1, le=1000) = Field(100, description="Items per page (max 1000)")
    
    @property
    def skip(self) -> int:
        """Alias for offset (compatibility)."""
        return self.offset


class ValidatedErrorResponse(BaseModel):
    """Standardized error response."""
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Human-readable error message")
    field: Optional[str] = Field(None, description="Field that caused the error")
    request_id: Optional[str] = Field(None, description="Request ID for tracing")


class TableOperationParams(BaseModel):
    """Parameters for table operations."""
    table_name: constr(min_length=1, max_length=255) = Field(..., description="Table name")
    operation: str = Field(..., description="Operation type: create, update, delete")
    
    @field_validator('table_name')
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        """Validate table name."""
        v = validate_no_sql_injection(v)
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', v):
            raise ValueError("Table name must start with a letter and contain only letters, numbers, and underscores")
        return v