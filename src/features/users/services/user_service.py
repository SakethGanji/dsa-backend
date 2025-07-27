"""Consolidated service for all user operations."""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.user_repo import PostgresUserRepository
from src.infrastructure.external.password_hasher import PasswordHasher
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus, DomainEvent
from src.core.domain_exceptions import (
    ConflictException, 
    EntityNotFoundException, 
    ValidationException,
    BusinessRuleViolation
)
from src.core.auth import create_access_token, create_refresh_token
from src.core.common.pagination import PaginationMixin
from ...base_handler import with_transaction, with_error_handling
from ..models import (
    CreateUserCommand,
    UpdateUserCommand,
    DeleteUserCommand,
    ListUsersCommand,
    User,
    UserRole,
    UserCredentials
)


@dataclass
class UserCreatedEvent(DomainEvent):
    """Event raised when a user is created."""
    user_id: int
    soeid: str
    role: str
    created_by: int


@dataclass
class UserUpdatedEvent(DomainEvent):
    """Event raised when a user is updated."""
    user_id: int
    updated_fields: List[str]
    updated_by: int


@dataclass
class UserDeletedEvent(DomainEvent):
    """Event raised when a user is deleted."""
    user_id: int
    deleted_by: int
    user_soeid: str


@dataclass
class LoginResponse:
    """Response for user login."""
    access_token: str
    refresh_token: str
    token_type: str
    user_id: int
    soeid: str
    role_id: int
    role_name: str


@dataclass
class UserResponse:
    """Response for user operations."""
    id: int
    soeid: str
    role_id: int
    role_name: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_login: Optional[datetime] = None
    is_active: Optional[bool] = True


@dataclass
class UserListItem:
    """Single item in user list."""
    id: int
    soeid: str
    role_id: int
    role_name: Optional[str]
    created_at: datetime
    last_login: Optional[datetime]
    dataset_count: Optional[int] = 0


@dataclass
class DeleteUserResponse:
    """Standardized delete response."""
    entity_type: str = "User"
    entity_id: int = None
    success: bool = True
    message: str = None


class UserService(PaginationMixin):
    """Consolidated service for all user operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        user_repo: PostgresUserRepository,
        permissions: PermissionService,
        password_hasher: Optional[PasswordHasher] = None,
        event_bus: Optional[EventBus] = None
    ):
        self._uow = uow
        self._user_repo = user_repo
        self._permissions = permissions
        self._password_hasher = password_hasher or PasswordHasher()
        self._event_bus = event_bus
    
    # ========== Public Methods (No Auth Required) ==========
    
    @with_error_handling
    async def login(
        self,
        soeid: str,
        password: str
    ) -> LoginResponse:
        """Authenticate user and generate tokens."""
        # Get user with password
        user_data = await self._user_repo.get_user_with_password(soeid)
        if not user_data:
            raise ValidationException("Invalid credentials")
        
        # Create domain model from repository data
        user = User.from_repository_data(user_data)
        
        # Validate user can login (checks status, etc.)
        try:
            user.validate_for_login()
        except BusinessRuleViolation:
            raise ValidationException("Invalid credentials")
        
        # Verify credentials using domain method
        if not user.verify_credentials(password, self._password_hasher):
            raise ValidationException("Invalid credentials")
        
        # Record successful login
        user.record_login()
        # Note: In a complete implementation, we would persist this through the repository
        
        # Generate tokens
        access_token = create_access_token(
            subject=user.soeid,
            role_id=user.role.to_id(),
            role_name=user.role.value
        )
        
        refresh_token = create_refresh_token(
            subject=user.soeid
        )
        
        return LoginResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            user_id=user.id,
            soeid=user.soeid,
            role_id=user.role.to_id(),
            role_name=user.role.value
        )
    
    # ========== Admin-Only Methods ==========
    
    @with_transaction
    @with_error_handling
    async def create_user(
        self,
        command: CreateUserCommand
    ) -> UserResponse:
        """Create a new user (admin only)."""
        # Check permissions - only admins can create users
        await self._permissions.require_role(command.created_by, "admin")
        
        # Check if user already exists
        await self._validate_unique_soeid(command.soeid)
        
        # Validate password using domain logic
        UserCredentials.validate_password(command.password)
        
        # Hash the password
        password_hash = self._password_hasher.hash_password(command.password)
        
        # Create domain model
        user = User(
            soeid=command.soeid,
            role=UserRole.from_id(command.role_id),
            credentials=UserCredentials(
                password_hash=password_hash
            )
        )
        
        # The domain model validates soeid in __post_init__
        
        # Persist user
        user_id = await self._user_repo.create_user(
            soeid=user.soeid,
            password_hash=user.credentials.password_hash,
            role_id=user.role.to_id()
        )
        
        # Get the created user details
        created_user_data = await self._user_repo.get_by_id(user_id)
        created_user = User.from_repository_data(created_user_data)
        
        # Publish event
        if self._event_bus:
            await self._event_bus.publish(UserCreatedEvent(
                user_id=user_id,
                soeid=created_user.soeid,
                role=created_user.role.value,
                created_by=command.created_by
            ))
        
        return UserResponse(
            id=created_user.id,
            soeid=created_user.soeid,
            role_id=created_user.role.to_id(),
            role_name=created_user.role.value,
            created_at=created_user.created_at
        )
    
    @with_error_handling
    async def list_users(
        self,
        command: ListUsersCommand
    ) -> Tuple[List[UserListItem], int]:
        """List all users with pagination (admin only)."""
        # Check permissions - only admins can list all users
        await self._permissions.require_role(command.user_id, "admin")
        
        # Validate pagination
        offset, limit = self.validate_pagination(command.offset, command.limit)
        
        # Get users
        users, total = await self._user_repo.list_users(
            offset=offset,
            limit=limit,
            search=command.search,
            role_id=command.role_id,
            sort_by=command.sort_by,
            sort_order=command.sort_order
        )
        
        # Convert to response models
        user_items = []
        for user in users:
            # Get dataset count for user if available
            dataset_count = 0
            if hasattr(self._uow, 'datasets'):
                dataset_count = await self._uow.datasets.count_datasets_for_user(user['id'])
            
            user_items.append(UserListItem(
                id=user['id'],
                soeid=user['soeid'],
                role_id=user['role_id'],
                role_name=user.get('role_name'),
                created_at=user['created_at'],
                last_login=user.get('last_login'),
                dataset_count=dataset_count
            ))
        
        return user_items, total
    
    @with_transaction
    @with_error_handling
    async def update_user(
        self,
        command: UpdateUserCommand
    ) -> UserResponse:
        """Update user information (admin only)."""
        # Check permissions - only admins can update users
        await self._permissions.require_role(command.user_id, "admin")
        
        # Get existing user
        existing = await self._user_repo.get_by_id(command.target_user_id)
        if not existing:
            raise EntityNotFoundException("User", command.target_user_id)
        
        # Create domain model from existing data
        user = User.from_repository_data(existing)
        
        # Prepare update data
        update_data = {}
        updated_fields = []
        
        # Validate and prepare soeid update
        if command.soeid is not None:
            await self._validate_unique_soeid(command.soeid, exclude_user_id=command.target_user_id)
            # Validate new soeid using domain logic
            try:
                user.update_soeid(command.soeid)
                update_data['soeid'] = command.soeid
                updated_fields.append('soeid')
            except BusinessRuleViolation as e:
                raise ConflictException(
                    str(e),
                    conflicting_field="soeid",
                    existing_value=command.soeid
                )
        
        # Validate and prepare password update
        if command.password is not None:
            UserCredentials.validate_password(command.password)
            update_data['password_hash'] = self._password_hasher.hash_password(command.password)
            updated_fields.append('password')
        
        # Prepare role update
        if command.role_id is not None:
            update_data['role_id'] = command.role_id
            updated_fields.append('role_id')
        
        # Perform update if there are changes
        if update_data:
            await self._user_repo.update_user(
                user_id=command.target_user_id,
                **update_data
            )
        
        # Get updated user
        updated_user_data = await self._user_repo.get_by_id(command.target_user_id)
        
        # Publish event
        if self._event_bus and updated_fields:
            await self._event_bus.publish(UserUpdatedEvent(
                user_id=command.target_user_id,
                updated_fields=updated_fields,
                updated_by=command.user_id
            ))
        
        return UserResponse(
            id=updated_user_data['id'],
            soeid=updated_user_data['soeid'],
            role_id=updated_user_data['role_id'],
            role_name=updated_user_data.get('role_name'),
            created_at=updated_user_data['created_at'],
            updated_at=updated_user_data.get('updated_at')
        )
    
    @with_transaction
    @with_error_handling
    async def delete_user(
        self,
        command: DeleteUserCommand
    ) -> DeleteUserResponse:
        """Delete a user (admin only)."""
        # Check permissions - only admins can delete users
        await self._permissions.require_role(command.user_id, "admin")
        
        # Prevent self-deletion
        if command.user_id == command.target_user_id:
            raise BusinessRuleViolation("Cannot delete your own user account", rule="no_self_deletion")
        
        # Check if target user exists
        user = await self._user_repo.get_by_id(command.target_user_id)
        if not user:
            raise EntityNotFoundException("User", command.target_user_id)
        
        # Check if user is the last admin
        if user.get('role_name') == 'admin':
            await self._ensure_admin_exists()
        
        # Delete the user
        await self._user_repo.delete_user(command.target_user_id)
        
        # Publish event
        if self._event_bus:
            await self._event_bus.publish(UserDeletedEvent(
                user_id=command.target_user_id,
                deleted_by=command.user_id,
                user_soeid=user['soeid']
            ))
        
        # Return standardized response
        return DeleteUserResponse(
            entity_type="User",
            entity_id=command.target_user_id,
            message=f"User '{user['soeid']}' has been deleted successfully"
        )
    
    # ========== Test-Only Methods ==========
    
    @with_transaction
    @with_error_handling
    async def create_user_public(
        self,
        soeid: str,
        password: str,
        role_id: Optional[int] = None
    ) -> UserResponse:
        """Create user without authentication (TEST ONLY - DO NOT USE IN PRODUCTION)."""
        # Check if user already exists
        await self._validate_unique_soeid(soeid)
        
        # Hash the password
        password_hash = self._password_hasher.hash_password(password)
        
        # Ensure role exists (default to admin role for testing)
        if not role_id:
            # Get or create admin role
            async with self._uow:
                role = await self._uow.connection.fetchrow("""
                    INSERT INTO dsa_auth.roles (role_name, description) 
                    VALUES ('admin', 'Administrator role')
                    ON CONFLICT (role_name) DO UPDATE SET role_name = EXCLUDED.role_name
                    RETURNING id
                """)
                role_id = role['id']
        
        # Create user
        user_id = await self._user_repo.create_user(
            soeid=soeid,
            password_hash=password_hash,
            role_id=role_id
        )
        
        # Get the created user details
        user = await self._user_repo.get_by_id(user_id)
        
        return UserResponse(
            id=user['id'],
            soeid=user['soeid'],
            role_id=user['role_id'],
            role_name=user.get('role_name'),
            is_active=user.get('is_active', True),
            created_at=user['created_at']
        )
    
    # ========== Private Helper Methods ==========
    
    async def _validate_unique_soeid(self, soeid: str, exclude_user_id: Optional[int] = None):
        """Validate SOEID is unique."""
        existing_user = await self._user_repo.get_by_soeid(soeid)
        if existing_user and existing_user['id'] != exclude_user_id:
            raise ConflictException(
                f"User with SOEID {soeid} already exists",
                entity_type="User",
                entity_id=soeid
            )
    
    async def _ensure_admin_exists(self):
        """Ensure at least one admin user exists."""
        admin_count = await self._user_repo.count_users_by_role('admin')
        if admin_count <= 1:
            raise BusinessRuleViolation(
                "Cannot delete the last admin user", 
                rule="maintain_admin_count"
            )