"""Base models and constants for the system."""

from .base import (
    # Base timestamp models
    TimestampedModel,
    CreatedAtModel,
    UpdatedAtModel,
    
    # Base identity models
    IdentifiedModel,
    UUIDIdentifiedModel,
    StringIdentifiedModel,
    
    # Base audit models
    AuditedModel,
    CreatedByModel,
    
    # Base feature models
    TaggedModel,
    MetadataModel,
    DescribedModel,
    NamedModel,
    
    # Composite base models
    BaseEntityModel,
    BaseAuditedEntityModel,
    BaseNamedEntityModel,
    
    # Domain-specific base models
    BaseDatasetModel,
    BaseUserModel,
    BaseJobModel,
    BaseCommitModel,
    BaseBranchModel,
    
    # Response wrapper models
    BasePaginatedResponse,
    BaseDetailResponse,
    BaseOperationResponse,
    BaseDeleteResponse,
    
    # Permission and status models
    BasePermissionModel,
    BaseStatusModel,
)

from .constants import (
    PermissionLevel,
    JobStatus,
    ImportStatus,
    PermissionType,
)

__all__ = [
    # Base timestamp models
    "TimestampedModel",
    "CreatedAtModel",
    "UpdatedAtModel",
    
    # Base identity models
    "IdentifiedModel",
    "UUIDIdentifiedModel",
    "StringIdentifiedModel",
    
    # Base audit models
    "AuditedModel",
    "CreatedByModel",
    
    # Base feature models
    "TaggedModel",
    "MetadataModel",
    "DescribedModel",
    "NamedModel",
    
    # Composite base models
    "BaseEntityModel",
    "BaseAuditedEntityModel",
    "BaseNamedEntityModel",
    
    # Domain-specific base models
    "BaseDatasetModel",
    "BaseUserModel",
    "BaseJobModel",
    "BaseCommitModel",
    "BaseBranchModel",
    
    # Response wrapper models
    "BasePaginatedResponse",
    "BaseDetailResponse",
    "BaseOperationResponse",
    "BaseDeleteResponse",
    
    # Permission and status models
    "BasePermissionModel",
    "BaseStatusModel",
    
    # Constants
    "PermissionLevel",
    "JobStatus",
    "ImportStatus",
    "PermissionType",
]