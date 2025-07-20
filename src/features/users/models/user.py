"""User domain models."""

from dataclasses import dataclass
from typing import Optional
from enum import Enum
from datetime import datetime

from src.core.domain_exceptions import ValidationException, BusinessRuleViolation


class UserStatus(Enum):
    """User account status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"
    DELETED = "deleted"


class UserRole(Enum):
    """User role enumeration."""
    ADMIN = "admin"
    USER = "user"
    VIEWER = "viewer"
    
    @classmethod
    def from_id(cls, role_id: int) -> 'UserRole':
        """Convert role ID to UserRole."""
        role_map = {
            1: cls.ADMIN,
            2: cls.USER,
            3: cls.VIEWER
        }
        return role_map.get(role_id, cls.VIEWER)
    
    def to_id(self) -> int:
        """Convert UserRole to role ID."""
        role_map = {
            UserRole.ADMIN: 1,
            UserRole.USER: 2,
            UserRole.VIEWER: 3
        }
        return role_map.get(self, 3)


@dataclass
class UserCredentials:
    """Value object for user credentials."""
    password_hash: str
    last_password_change: Optional[datetime] = None
    password_reset_required: bool = False
    
    @staticmethod
    def validate_password(password: str) -> None:
        """Validate password meets requirements."""
        if not password or len(password) < 8:
            raise ValidationException(
                "Password must be at least 8 characters long",
                field="password"
            )
        if len(password) > 128:
            raise ValidationException(
                "Password cannot exceed 128 characters",
                field="password"
            )
        # Add more password complexity rules as needed


@dataclass
class User:
    """User aggregate root entity."""
    id: Optional[int] = None
    soeid: str = ""
    role: UserRole = UserRole.VIEWER
    status: UserStatus = UserStatus.ACTIVE
    credentials: Optional[UserCredentials] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_login_at: Optional[datetime] = None
    
    @classmethod
    def from_repository_data(cls, data: dict) -> 'User':
        """Create User entity from repository data."""
        credentials = None
        if 'password_hash' in data and data['password_hash']:
            credentials = UserCredentials(
                password_hash=data['password_hash'],
                last_password_change=data.get('last_password_change'),
                password_reset_required=data.get('password_reset_required', False)
            )
        
        return cls(
            id=data['id'],
            soeid=data['soeid'],
            role=UserRole.from_id(data['role_id']),
            status=UserStatus(data.get('status', 'active')),
            credentials=credentials,
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at'),
            last_login_at=data.get('last_login_at')
        )
    
    def __post_init__(self):
        """Initialize and validate user."""
        if self.id is None:  # New user
            self._validate_soeid()
    
    def _validate_soeid(self) -> None:
        """Validate SOEID format."""
        if not self.soeid or len(self.soeid.strip()) == 0:
            raise ValidationException("SOEID is required", field="soeid")
        
        if len(self.soeid) < 2:
            raise ValidationException("SOEID must be at least 2 characters", field="soeid")
        
        if len(self.soeid) > 50:
            raise ValidationException("SOEID cannot exceed 50 characters", field="soeid")
        
        if not self.soeid.isalnum():
            raise ValidationException(
                "SOEID can only contain alphanumeric characters",
                field="soeid"
            )
        
        self.soeid = self.soeid.lower().strip()
    
    def is_active(self) -> bool:
        """Check if user account is active."""
        return self.status == UserStatus.ACTIVE
    
    def is_admin(self) -> bool:
        """Check if user has admin role."""
        return self.role == UserRole.ADMIN
    
    def is_viewer(self) -> bool:
        """Check if user has viewer role."""
        return self.role == UserRole.VIEWER
    
    def can_write(self) -> bool:
        """Check if user can write data."""
        return self.role in [UserRole.ADMIN, UserRole.USER]
    
    def can_manage_users(self) -> bool:
        """Check if user can manage other users."""
        return self.role == UserRole.ADMIN
    
    def can_grant_permissions(self) -> bool:
        """Check if user can grant dataset permissions."""
        return self.role == UserRole.ADMIN
    
    def update_password(self, new_password_hash: str) -> None:
        """Update user password."""
        if self.status != UserStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot update password for {self.status.value} user",
                rule="user_must_be_active"
            )
        
        if not self.credentials:
            self.credentials = UserCredentials(password_hash=new_password_hash)
        else:
            self.credentials.password_hash = new_password_hash
            self.credentials.last_password_change = datetime.utcnow()
            self.credentials.password_reset_required = False
    
    def require_password_reset(self) -> None:
        """Mark that user must reset password on next login."""
        if self.credentials:
            self.credentials.password_reset_required = True
    
    def update_role(self, new_role: UserRole) -> None:
        """Update user role."""
        if self.status != UserStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot update role for {self.status.value} user",
                rule="user_must_be_active"
            )
        
        if self.id == 1 and new_role != UserRole.ADMIN:
            raise BusinessRuleViolation(
                "Cannot change role of system admin user",
                rule="protect_system_admin"
            )
        
        self.role = new_role
    
    def update_soeid(self, new_soeid: str) -> None:
        """Update user SOEID."""
        if self.status != UserStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot update SOEID for {self.status.value} user",
                rule="user_must_be_active"
            )
        
        self.soeid = new_soeid
        self._validate_soeid()
    
    def suspend(self) -> None:
        """Suspend user account."""
        if self.status != UserStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot suspend {self.status.value} user",
                rule="user_must_be_active"
            )
        
        if self.id == 1:
            raise BusinessRuleViolation(
                "Cannot suspend system admin user",
                rule="protect_system_admin"
            )
        
        self.status = UserStatus.SUSPENDED
    
    def reactivate(self) -> None:
        """Reactivate suspended user account."""
        if self.status != UserStatus.SUSPENDED:
            raise BusinessRuleViolation(
                f"Can only reactivate suspended users, not {self.status.value}",
                rule="user_must_be_suspended"
            )
        
        self.status = UserStatus.ACTIVE
    
    def can_be_deleted(self) -> bool:
        """Check if user can be deleted."""
        if self.status == UserStatus.DELETED:
            return False
        
        if self.id == 1:  # System admin
            return False
        
        # Add more business rules here if needed
        # e.g., check for owned datasets, active sessions, etc.
        return True
    
    def mark_as_deleted(self) -> None:
        """Mark user as deleted (soft delete)."""
        if not self.can_be_deleted():
            raise BusinessRuleViolation(
                "User cannot be deleted",
                rule="user_deletion_requirements"
            )
        
        self.status = UserStatus.DELETED
    
    def record_login(self) -> None:
        """Record successful login."""
        self.last_login_at = datetime.utcnow()
    
    def validate_for_login(self) -> None:
        """Validate user can login."""
        if self.status == UserStatus.DELETED:
            raise BusinessRuleViolation(
                "Cannot login with deleted account",
                rule="user_must_exist"
            )
        
        if self.status == UserStatus.SUSPENDED:
            raise BusinessRuleViolation(
                "Account is suspended",
                rule="user_must_be_active"
            )
        
        if self.status != UserStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot login with {self.status.value} account",
                rule="user_must_be_active"
            )
    
    def verify_credentials(self, password: str, password_manager) -> bool:
        """Verify user credentials.
        
        Args:
            password: Plain text password to verify
            password_manager: Password manager instance for verification
            
        Returns:
            True if credentials are valid, False otherwise
        """
        if not self.credentials or not self.credentials.password_hash:
            return False
        
        return password_manager.verify_password(password, self.credentials.password_hash)