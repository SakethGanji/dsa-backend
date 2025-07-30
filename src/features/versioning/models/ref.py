"""Versioning ref (branch/tag) domain models."""

from dataclasses import dataclass
from typing import Optional
from enum import Enum
from datetime import datetime

from src.core.domain_exceptions import ValidationException, BusinessRuleViolation


class RefType(Enum):
    """Reference type enumeration."""
    BRANCH = "branch"
    TAG = "tag"


@dataclass
class Ref:
    """Reference entity pointing to a commit."""
    dataset_id: int
    name: str
    ref_type: RefType
    commit_id: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[int] = None
    
    def __post_init__(self):
        """Validate ref."""
        self._validate_name()
        self._validate_commit_id()
    
    def _validate_name(self) -> None:
        """Validate ref name."""
        if not self.name or len(self.name.strip()) == 0:
            raise ValidationException("Ref name is required", field="name")
        
        if len(self.name) > 100:
            raise ValidationException("Ref name cannot exceed 100 characters", field="name")
        
        # Different validation rules for branches and tags
        if self.ref_type == RefType.BRANCH:
            self._validate_branch_name()
        else:
            self._validate_tag_name()
    
    def _validate_branch_name(self) -> None:
        """Validate branch name format."""
        # Branch names can contain alphanumeric, hyphens, underscores, and slashes
        valid_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_/")
        if not all(c in valid_chars for c in self.name):
            raise ValidationException(
                "Branch name can only contain alphanumeric characters, hyphens, underscores, and slashes",
                field="name"
            )
        
        # Cannot start or end with slash
        if self.name.startswith("/") or self.name.endswith("/"):
            raise ValidationException(
                "Branch name cannot start or end with a slash",
                field="name"
            )
        
        # Cannot have consecutive slashes
        if "//" in self.name:
            raise ValidationException(
                "Branch name cannot contain consecutive slashes",
                field="name"
            )
        
        # Reserved branch names
        reserved_names = {"HEAD", "head"}
        if self.name in reserved_names:
            raise ValidationException(
                f"'{self.name}' is a reserved branch name",
                field="name"
            )
    
    def _validate_tag_name(self) -> None:
        """Validate tag name format."""
        # Tag names have similar rules to branches but typically use dots for versioning
        valid_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_/.")
        if not all(c in valid_chars for c in self.name):
            raise ValidationException(
                "Tag name can only contain alphanumeric characters, hyphens, underscores, slashes, and dots",
                field="name"
            )
        
        # Cannot start or end with slash or dot
        if self.name.startswith("/") or self.name.endswith("/"):
            raise ValidationException(
                "Tag name cannot start or end with a slash",
                field="name"
            )
        if self.name.startswith(".") or self.name.endswith("."):
            raise ValidationException(
                "Tag name cannot start or end with a dot",
                field="name"
            )
    
    def _validate_commit_id(self) -> None:
        """Validate commit ID format."""
        if not self.commit_id or len(self.commit_id.strip()) == 0:
            raise ValidationException("Commit ID is required", field="commit_id")
        
        # Commit ID should be a valid SHA-256 hash (64 hex characters)
        if len(self.commit_id) != 64:
            raise ValidationException(
                "Commit ID must be 64 characters (SHA-256 hash)",
                field="commit_id"
            )
        
        try:
            int(self.commit_id, 16)  # Validate it's a hex string
        except ValueError:
            raise ValidationException(
                "Commit ID must be a valid hexadecimal string",
                field="commit_id"
            )
    
    def is_branch(self) -> bool:
        """Check if this ref is a branch."""
        return self.ref_type == RefType.BRANCH
    
    def is_tag(self) -> bool:
        """Check if this ref is a tag."""
        return self.ref_type == RefType.TAG
    
    def update_commit(self, new_commit_id: str, expected_commit_id: Optional[str] = None) -> None:
        """Update ref to point to a new commit."""
        if self.ref_type == RefType.TAG:
            raise BusinessRuleViolation(
                "Cannot update tag reference - tags are immutable",
                rule="tags_are_immutable"
            )
        
        if expected_commit_id and self.commit_id != expected_commit_id:
            raise BusinessRuleViolation(
                f"Ref has been updated by another process. Expected {expected_commit_id}, but current is {self.commit_id}",
                rule="optimistic_locking"
            )
        
        old_commit = self.commit_id
        self.commit_id = new_commit_id
        self._validate_commit_id()
        self.updated_at = datetime.utcnow()
        
        return old_commit
    
    def can_be_deleted(self) -> bool:
        """Check if ref can be deleted."""
        # Cannot delete the default branch (usually 'main')
        if self.name == "main" and self.ref_type == RefType.BRANCH:
            return False
        
        # Tags can always be deleted (though it's generally discouraged)
        # Additional business rules can be added here
        return True
    
    @staticmethod
    def create_branch(dataset_id: int, name: str, commit_id: str, created_by: int) -> 'Ref':
        """Factory method to create a branch."""
        return Ref(
            dataset_id=dataset_id,
            name=name,
            ref_type=RefType.BRANCH,
            commit_id=commit_id,
            created_at=datetime.utcnow(),
            created_by=created_by
        )
    
    @staticmethod
    def create_tag(dataset_id: int, name: str, commit_id: str, created_by: int) -> 'Ref':
        """Factory method to create a tag."""
        return Ref(
            dataset_id=dataset_id,
            name=name,
            ref_type=RefType.TAG,
            commit_id=commit_id,
            created_at=datetime.utcnow(),
            created_by=created_by
        )