"""Handler for creating new users."""

from typing import Dict, Any, Optional
from ....infrastructure.postgres.uow import PostgresUnitOfWork
from ....infrastructure.postgres.user_repo import PostgresUserRepository
from ....core.events.publisher import EventBus
from ....core.events.publisher import DomainEvent
from dataclasses import dataclass


@dataclass
class UserCreatedEvent(DomainEvent):
    """Event raised when a user is created."""
    user_id: int
    soeid: str
    role: str
    created_by: int
from ....infrastructure.external.password_hasher import PasswordHasher
from ....api.models.requests import CreateUserRequest
from ....api.models.responses import CreateUserResponse
from ....features.base_handler import BaseHandler, with_transaction
from ....core.decorators import requires_role
from ....core.domain_exceptions import ConflictException
from ..models import CreateUserCommand, User, UserRole, UserCredentials


class CreateUserHandler(BaseHandler):
    """Handler for creating new users with proper password hashing."""
    
    def __init__(self, uow: PostgresUnitOfWork, user_repo: PostgresUserRepository, password_manager: PasswordHasher = None, event_bus: Optional[EventBus] = None):
        super().__init__(uow)
        self._user_repo = user_repo
        self._password_manager = password_manager or PasswordHasher()
        self._event_bus = event_bus
    
    @with_transaction
    @requires_role("admin")  # Only admins can create users
    async def handle(self, command: CreateUserCommand) -> CreateUserResponse:
        """Create a new user with hashed password."""
        # Check if user already exists
        existing_user = await self._user_repo.get_by_soeid(command.soeid)
        if existing_user:
            raise ConflictException(
                f"User with SOEID {command.soeid} already exists",
                entity_type="User",
                entity_id=command.soeid
            )
        
        # Validate password using domain logic
        UserCredentials.validate_password(command.password)
        
        # Hash the password
        password_hash = self._password_manager.hash_password(command.password)
        
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
        
        return CreateUserResponse(
            id=created_user.id,
            soeid=created_user.soeid,
            role_id=created_user.role.to_id(),
            role_name=created_user.role.value,
            created_at=created_user.created_at
        )