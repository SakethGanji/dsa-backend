"""Dataset domain models."""

from dataclasses import dataclass, field
from typing import List, Optional, Set
from enum import Enum
from datetime import datetime

from src.core.domain_exceptions import ValidationException, BusinessRuleViolation


class DatasetStatus(Enum):
    """Dataset status enumeration."""
    ACTIVE = "active"
    DELETED = "deleted"
    ARCHIVED = "archived"


@dataclass
class DatasetTag:
    """Value object for dataset tags."""
    value: str
    
    def __post_init__(self):
        """Validate tag value."""
        if not self.value or len(self.value.strip()) == 0:
            raise ValidationException("Tag cannot be empty", field="tag")
        if len(self.value) > 50:
            raise ValidationException("Tag cannot exceed 50 characters", field="tag")
        if not self.value.replace("-", "").replace("_", "").isalnum():
            raise ValidationException("Tag can only contain alphanumeric characters, hyphens, and underscores", field="tag")
        self.value = self.value.lower().strip()


@dataclass
class DatasetMetadata:
    """Value object for dataset metadata."""
    created_at: datetime
    created_by: int
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    last_commit_at: Optional[datetime] = None
    commit_count: int = 0


@dataclass
class DatasetPermission:
    """Entity for dataset permissions."""
    id: Optional[int] = None
    dataset_id: int = 0
    user_id: int = 0
    permission_type: str = ""
    granted_at: Optional[datetime] = None
    granted_by: Optional[int] = None
    
    def is_read_permission(self) -> bool:
        """Check if this is a read permission."""
        return self.permission_type == "read"
    
    def is_write_permission(self) -> bool:
        """Check if this is a write permission."""
        return self.permission_type == "write"
    
    def is_admin_permission(self) -> bool:
        """Check if this is an admin permission."""
        return self.permission_type == "admin"


@dataclass
class Dataset:
    """Dataset aggregate root entity."""
    id: Optional[int] = None
    name: str = ""
    description: Optional[str] = None
    status: DatasetStatus = DatasetStatus.ACTIVE
    tags: List[DatasetTag] = field(default_factory=list)
    metadata: Optional[DatasetMetadata] = None
    permissions: List[DatasetPermission] = field(default_factory=list)
    default_branch: str = "main"
    
    def __post_init__(self):
        """Initialize and validate dataset."""
        if self.id is None:  # New dataset
            self._validate_name()
            self._validate_description()
            self._validate_default_branch()
    
    def _validate_name(self):
        """Validate dataset name."""
        if not self.name or len(self.name.strip()) == 0:
            raise ValidationException("Dataset name is required", field="name")
        if len(self.name) > 255:
            raise ValidationException("Dataset name cannot exceed 255 characters", field="name")
        if not self.name.replace("-", "").replace("_", "").replace(" ", "").isalnum():
            raise ValidationException(
                "Dataset name can only contain alphanumeric characters, spaces, hyphens, and underscores", 
                field="name"
            )
    
    def _validate_description(self):
        """Validate dataset description."""
        if self.description and len(self.description) > 1000:
            raise ValidationException("Description cannot exceed 1000 characters", field="description")
    
    def _validate_default_branch(self):
        """Validate default branch name."""
        if not self.default_branch or len(self.default_branch.strip()) == 0:
            raise ValidationException("Default branch is required", field="default_branch")
        if len(self.default_branch) > 100:
            raise ValidationException("Default branch name cannot exceed 100 characters", field="default_branch")
        if not self.default_branch.replace("-", "").replace("_", "").replace("/", "").isalnum():
            raise ValidationException(
                "Branch name can only contain alphanumeric characters, hyphens, underscores, and slashes",
                field="default_branch"
            )
    
    def add_tag(self, tag_value: str) -> None:
        """Add a tag to the dataset."""
        if self.status != DatasetStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot add tags to {self.status.value} dataset",
                rule="dataset_must_be_active"
            )
        
        tag = DatasetTag(tag_value)
        
        # Check for duplicates
        if any(t.value == tag.value for t in self.tags):
            raise BusinessRuleViolation(
                f"Tag '{tag.value}' already exists on this dataset",
                rule="no_duplicate_tags"
            )
        
        # Check tag limit
        if len(self.tags) >= 20:
            raise BusinessRuleViolation(
                "Cannot add more than 20 tags to a dataset",
                rule="tag_limit"
            )
        
        self.tags.append(tag)
    
    def remove_tag(self, tag_value: str) -> None:
        """Remove a tag from the dataset."""
        if self.status != DatasetStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot remove tags from {self.status.value} dataset",
                rule="dataset_must_be_active"
            )
        
        tag_value = tag_value.lower().strip()
        self.tags = [t for t in self.tags if t.value != tag_value]
    
    def clear_tags(self) -> None:
        """Remove all tags from the dataset."""
        if self.status != DatasetStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot clear tags from {self.status.value} dataset",
                rule="dataset_must_be_active"
            )
        self.tags = []
    
    def update_info(self, name: Optional[str] = None, description: Optional[str] = None) -> None:
        """Update dataset information."""
        if self.status != DatasetStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot update {self.status.value} dataset",
                rule="dataset_must_be_active"
            )
        
        if name is not None:
            self.name = name
            self._validate_name()
        
        if description is not None:
            self.description = description
            self._validate_description()
    
    def can_be_deleted(self) -> bool:
        """Check if dataset can be deleted."""
        if self.status != DatasetStatus.ACTIVE:
            return False
        
        # Add more business rules here if needed
        # e.g., check for active jobs, dependent datasets, etc.
        return True
    
    def mark_as_deleted(self) -> None:
        """Mark dataset as deleted."""
        if not self.can_be_deleted():
            raise BusinessRuleViolation(
                "Dataset cannot be deleted in its current state",
                rule="dataset_deletion_requirements"
            )
        self.status = DatasetStatus.DELETED
    
    def grant_permission(self, user_id: int, permission_type: str, granted_by: int) -> None:
        """Grant permission to a user."""
        if self.status != DatasetStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot grant permissions on {self.status.value} dataset",
                rule="dataset_must_be_active"
            )
        
        if permission_type not in ["read", "write", "admin"]:
            raise ValidationException(
                f"Invalid permission type: {permission_type}",
                field="permission_type"
            )
        
        # Check if permission already exists
        existing = next(
            (p for p in self.permissions 
             if p.user_id == user_id and p.permission_type == permission_type),
            None
        )
        
        if existing:
            raise BusinessRuleViolation(
                f"User {user_id} already has {permission_type} permission",
                rule="no_duplicate_permissions"
            )
        
        permission = DatasetPermission(
            dataset_id=self.id,
            user_id=user_id,
            permission_type=permission_type,
            granted_at=datetime.utcnow(),
            granted_by=granted_by
        )
        
        self.permissions.append(permission)
    
    def revoke_permission(self, user_id: int, permission_type: str) -> None:
        """Revoke permission from a user."""
        if self.status != DatasetStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot revoke permissions on {self.status.value} dataset",
                rule="dataset_must_be_active"
            )
        
        self.permissions = [
            p for p in self.permissions 
            if not (p.user_id == user_id and p.permission_type == permission_type)
        ]
    
    def has_permission(self, user_id: int, permission_type: str) -> bool:
        """Check if user has specific permission."""
        return any(
            p.user_id == user_id and p.permission_type == permission_type
            for p in self.permissions
        )
    
    def get_user_permissions(self, user_id: int) -> Set[str]:
        """Get all permissions for a user."""
        return {
            p.permission_type 
            for p in self.permissions 
            if p.user_id == user_id
        }