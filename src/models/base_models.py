"""Base models for response inheritance to reduce duplication."""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Dict, Any, TypeVar, Generic
from uuid import UUID

# Type variable for generic models
T = TypeVar('T')


# ============================================
# Base Timestamp Models
# ============================================

class TimestampedModel(BaseModel):
    """Base model with timestamp fields."""
    created_at: datetime
    updated_at: datetime


class CreatedAtModel(BaseModel):
    """Base model with only created_at timestamp."""
    created_at: datetime


class UpdatedAtModel(BaseModel):
    """Base model with only updated_at timestamp."""
    updated_at: datetime


# ============================================
# Base Identity Models
# ============================================

class IdentifiedModel(BaseModel):
    """Base model with ID field."""
    id: int


class UUIDIdentifiedModel(BaseModel):
    """Base model with UUID ID field."""
    id: UUID


class StringIdentifiedModel(BaseModel):
    """Base model with string ID field."""
    id: str


# ============================================
# Base Audit Models
# ============================================

class AuditedModel(TimestampedModel):
    """Base model with audit fields."""
    created_by: int
    updated_by: Optional[int] = None


class CreatedByModel(BaseModel):
    """Base model with created_by field."""
    created_by: int


# ============================================
# Base Feature Models
# ============================================

class TaggedModel(BaseModel):
    """Base model with tags."""
    tags: List[str] = Field(default_factory=list)


class MetadataModel(BaseModel):
    """Base model with metadata."""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DescribedModel(BaseModel):
    """Base model with description."""
    description: Optional[str] = None


class NamedModel(BaseModel):
    """Base model with name."""
    name: str


# ============================================
# Composite Base Models
# ============================================

class BaseEntityModel(IdentifiedModel, TimestampedModel):
    """Base for all entity models with ID and timestamps."""
    
    class Config:
        """Allow field population by name."""
        populate_by_name = True


class BaseAuditedEntityModel(BaseEntityModel, AuditedModel):
    """Base for audited entities with full tracking."""
    pass


class BaseNamedEntityModel(BaseEntityModel, NamedModel, DescribedModel):
    """Base for named entities with description."""
    pass


# ============================================
# Domain-Specific Base Models
# ============================================

class BaseDatasetModel(BaseEntityModel, NamedModel, DescribedModel, TaggedModel):
    """Base for dataset-related models."""
    dataset_id: int = Field(alias="id")
    
    class Config:
        """Allow field population by name."""
        populate_by_name = True
        allow_population_by_field_name = True


class BaseUserModel(BaseEntityModel):
    """Base for user-related models."""
    user_id: int = Field(alias="id")
    soeid: str
    role_id: int
    role_name: Optional[str] = None
    
    class Config:
        """Allow field population by name."""
        populate_by_name = True
        allow_population_by_field_name = True


class BaseJobModel(BaseModel):
    """Base for job-related models."""
    job_id: str = Field(alias="id", description="UUID as string")
    run_type: str
    status: str
    dataset_id: Optional[int] = None
    user_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        """Allow field population by name."""
        populate_by_name = True
        allow_population_by_field_name = True


class BaseCommitModel(BaseModel):
    """Base for commit-related models."""
    commit_id: str
    parent_commit_id: Optional[str] = None
    message: str
    author_id: int
    created_at: datetime


class BaseBranchModel(BaseModel):
    """Base for branch/ref-related models."""
    ref_name: str
    commit_id: str
    dataset_id: int
    created_at: datetime
    updated_at: datetime


# ============================================
# Response Wrapper Models
# ============================================

class BasePaginatedResponse(BaseModel, Generic[T]):
    """Base paginated response for lists."""
    items: List[T]
    total: int
    offset: int
    limit: int
    has_more: bool = Field(computed=True)
    
    @property
    def has_more(self) -> bool:
        """Compute if there are more items."""
        return self.total > self.offset + len(self.items)
    
    class Config:
        """Allow generic types."""
        arbitrary_types_allowed = True


class BaseDetailResponse(BaseModel, Generic[T]):
    """Base response for single entity details."""
    data: T
    
    class Config:
        """Allow generic types."""
        arbitrary_types_allowed = True


class BaseOperationResponse(BaseModel):
    """Base response for operations."""
    success: bool = True
    message: str


class BaseDeleteResponse(BaseOperationResponse):
    """Base response for delete operations."""
    entity_type: str
    entity_id: Any
    message: str = Field(default="Entity deleted successfully")
    
    def __init__(self, **data):
        if 'message' not in data and 'entity_type' in data and 'entity_id' in data:
            data['message'] = f"{data['entity_type']} {data['entity_id']} deleted successfully"
        super().__init__(**data)


# ============================================
# Permission Models
# ============================================

class BasePermissionModel(BaseModel):
    """Base model for permission-related responses."""
    permission_type: str
    granted_at: Optional[datetime] = None
    granted_by: Optional[int] = None


class PermissionLevel:
    """Permission level constants."""
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"
    OWNER = "owner"


# ============================================
# Status Models
# ============================================

class BaseStatusModel(BaseModel):
    """Base model for status tracking."""
    status: str
    status_message: Optional[str] = None
    last_status_change: Optional[datetime] = None


class JobStatus:
    """Job status constants."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ImportStatus:
    """Import status constants."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"