"""Enhanced validation models with comprehensive constraints for API endpoints."""

from pydantic import BaseModel, Field, validator, constr, conint
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
from uuid import UUID
from enum import Enum
import re


# Custom validators
def validate_no_sql_injection(value: str) -> str:
    """Validate string doesn't contain SQL injection patterns."""
    sql_patterns = [
        r"(\b(union|select|insert|update|delete|drop|create|alter|exec|execute)\b)",
        r"(--|;|\/\*|\*\/|xp_|sp_)",
        r"(\bor\b\s*\d+\s*=\s*\d+)",
        r"(\band\b\s*\d+\s*=\s*\d+)"
    ]
    
    for pattern in sql_patterns:
        if re.search(pattern, value.lower()):
            raise ValueError("Potential SQL injection detected")
    return value


def validate_no_script_tags(value: str) -> str:
    """Validate string doesn't contain script tags."""
    if re.search(r"<\s*script", value, re.IGNORECASE):
        raise ValueError("Script tags not allowed")
    return value


def validate_safe_filename(filename: str) -> str:
    """Validate filename is safe."""
    # Check for path traversal
    if ".." in filename or "/" in filename or "\\" in filename:
        raise ValueError("Invalid filename: path traversal detected")
    
    # Check extension
    allowed_extensions = ['.csv', '.xlsx', '.xls', '.json', '.parquet']
    if not any(filename.lower().endswith(ext) for ext in allowed_extensions):
        raise ValueError(f"Invalid file extension. Allowed: {allowed_extensions}")
    
    return filename


# Enhanced Dataset Models
class EnhancedCreateDatasetRequest(BaseModel):
    name: constr(min_length=1, max_length=255, strip_whitespace=True) = Field(
        ..., 
        description="Dataset name (1-255 characters)",
        example="Q4 2024 Financial Data"
    )
    description: Optional[constr(max_length=1000, strip_whitespace=True)] = Field(
        None,
        description="Dataset description (max 1000 characters)",
        example="Quarterly financial metrics for Q4 2024"
    )
    tags: Optional[List[constr(min_length=1, max_length=50, strip_whitespace=True)]] = Field(
        default=[],
        description="List of tags (max 10 tags, each max 50 chars)",
        max_items=10,
        example=["financial", "quarterly", "2024"]
    )
    
    @validator('name')
    def validate_name(cls, v):
        v = validate_no_sql_injection(v)
        v = validate_no_script_tags(v)
        # Additional validation: no special characters except spaces, hyphens, underscores
        if not re.match(r'^[\w\s\-\.]+$', v):
            raise ValueError("Name can only contain letters, numbers, spaces, hyphens, dots and underscores")
        return v
    
    @validator('description')
    def validate_description(cls, v):
        if v:
            v = validate_no_sql_injection(v)
            v = validate_no_script_tags(v)
        return v
    
    @validator('tags', each_item=True)
    def validate_tags(cls, v):
        if v:
            v = validate_no_sql_injection(v)
            v = validate_no_script_tags(v)
            # Tags should be alphanumeric with hyphens
            if not re.match(r'^[\w\-]+$', v):
                raise ValueError("Tags can only contain letters, numbers, and hyphens")
        return v


class EnhancedUpdateDatasetRequest(BaseModel):
    name: Optional[constr(min_length=1, max_length=255, strip_whitespace=True)] = Field(
        None,
        description="New dataset name"
    )
    description: Optional[constr(max_length=1000, strip_whitespace=True)] = Field(
        None,
        description="New dataset description"
    )
    tags: Optional[List[constr(min_length=1, max_length=50, strip_whitespace=True)]] = Field(
        None,
        description="New list of tags (replaces existing tags)",
        max_items=10
    )
    
    @validator('name', 'description', 'tags')
    def validate_at_least_one_field(cls, v, values):
        if not any(values.get(field) is not None for field in ['name', 'description', 'tags']):
            raise ValueError("At least one field must be provided for update")
        return v
    
    # Apply same validators as create
    _validate_name = validator('name', allow_reuse=True)(EnhancedCreateDatasetRequest.validate_name)
    _validate_description = validator('description', allow_reuse=True)(EnhancedCreateDatasetRequest.validate_description)
    _validate_tags = validator('tags', allow_reuse=True)(EnhancedCreateDatasetRequest.validate_tags)


# Enhanced Permission Models
class EnhancedGrantPermissionRequest(BaseModel):
    user_id: conint(gt=0) = Field(
        ...,
        description="User ID to grant permission to",
        example=123
    )
    permission_type: str = Field(
        ...,
        description="Permission type",
        example="read"
    )
    
    @validator('permission_type')
    def validate_permission_type(cls, v):
        allowed = ['read', 'write', 'admin']
        if v not in allowed:
            raise ValueError(f"Permission type must be one of: {allowed}")
        return v


# Enhanced User Models
class EnhancedCreateUserRequest(BaseModel):
    soeid: constr(regex=r'^[A-Z0-9]{7}$') = Field(
        ...,
        description="SOEID (7 alphanumeric characters, uppercase)",
        example="ABCD123"
    )
    password: constr(min_length=8, max_length=128) = Field(
        ...,
        description="Password (8-128 characters)"
    )
    role_id: conint(gt=0) = Field(
        ...,
        description="Role ID",
        example=1
    )
    
    @validator('password')
    def validate_password_strength(cls, v):
        # At least one uppercase, one lowercase, one digit, one special character
        if not re.search(r'[A-Z]', v):
            raise ValueError("Password must contain at least one uppercase letter")
        if not re.search(r'[a-z]', v):
            raise ValueError("Password must contain at least one lowercase letter")
        if not re.search(r'\d', v):
            raise ValueError("Password must contain at least one digit")
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
            raise ValueError("Password must contain at least one special character")
        return v


class EnhancedLoginRequest(BaseModel):
    soeid: constr(regex=r'^[A-Z0-9]{7}$') = Field(
        ...,
        description="SOEID",
        example="ABCD123"
    )
    password: constr(min_length=1, max_length=128) = Field(
        ...,
        description="Password"
    )


# Enhanced Commit Models
class EnhancedCreateCommitRequest(BaseModel):
    parent_commit_id: Optional[constr(regex=r'^[a-f0-9]{8,40}$')] = Field(
        None,
        description="Parent commit ID (8-40 hex characters)",
        example="abc123def456"
    )
    message: constr(min_length=1, max_length=500, strip_whitespace=True) = Field(
        ...,
        description="Commit message (1-500 characters)",
        example="Updated Q4 revenue figures"
    )
    data: List[Dict[str, Any]] = Field(
        ...,
        description="Data rows to commit",
        min_items=1,
        max_items=100000  # Limit to prevent DoS
    )
    
    @validator('message')
    def validate_message(cls, v):
        v = validate_no_sql_injection(v)
        v = validate_no_script_tags(v)
        return v
    
    @validator('data')
    def validate_data(cls, v):
        # Validate each row has consistent keys
        if not v:
            return v
            
        first_keys = set(v[0].keys())
        for i, row in enumerate(v[1:], 1):
            if set(row.keys()) != first_keys:
                raise ValueError(f"Row {i} has different keys than first row")
        
        # Validate data types and sizes
        for row in v:
            for key, value in row.items():
                # Key validation
                if not isinstance(key, str):
                    raise ValueError("All keys must be strings")
                if len(key) > 100:
                    raise ValueError("Key names must be less than 100 characters")
                
                # Value validation
                if isinstance(value, str) and len(value) > 10000:
                    raise ValueError("String values must be less than 10000 characters")
                
                # Check for potentially dangerous content in strings
                if isinstance(value, str):
                    validate_no_script_tags(value)
        
        return v


# Enhanced Query Models
class EnhancedGetDataRequest(BaseModel):
    sheet_name: Optional[constr(min_length=1, max_length=100, strip_whitespace=True)] = Field(
        None,
        description="Filter by sheet name",
        example="Revenue"
    )
    offset: conint(ge=0, le=1000000) = Field(
        0,
        description="Pagination offset",
        example=0
    )
    limit: conint(ge=1, le=10000) = Field(
        100,
        description="Number of rows to return (max 10000)",
        example=100
    )
    
    @validator('sheet_name')
    def validate_sheet_name(cls, v):
        if v:
            v = validate_no_sql_injection(v)
            if not re.match(r'^[\w\s\-\.]+$', v):
                raise ValueError("Sheet name can only contain letters, numbers, spaces, hyphens, dots and underscores")
        return v


# File Import Models
class EnhancedQueueImportRequest(BaseModel):
    commit_message: constr(min_length=1, max_length=500, strip_whitespace=True) = Field(
        ...,
        description="Import commit message",
        example="Importing Q4 2024 financial data"
    )
    
    @validator('commit_message')
    def validate_commit_message(cls, v):
        v = validate_no_sql_injection(v)
        v = validate_no_script_tags(v)
        return v


# Pagination Models
class PaginationParams(BaseModel):
    offset: conint(ge=0, le=1000000) = Field(
        0,
        description="Pagination offset",
        example=0
    )
    limit: conint(ge=1, le=1000) = Field(
        50,
        description="Items per page (max 1000)",
        example=50
    )


# Response validation
class ValidatedErrorResponse(BaseModel):
    detail: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Application-specific error code")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    request_id: Optional[str] = Field(None, description="Request tracking ID")


# Table operation models
class TableOperationParams(BaseModel):
    dataset_id: conint(gt=0) = Field(..., description="Dataset ID")
    ref_name: constr(min_length=1, max_length=100) = Field(..., description="Reference name")
    table_key: constr(min_length=1, max_length=100) = Field(..., description="Table key")
    
    @validator('ref_name', 'table_key')
    def validate_names(cls, v):
        if not re.match(r'^[\w\-\.]+$', v):
            raise ValueError("Name can only contain letters, numbers, hyphens, dots and underscores")
        return v