"""Handler for updating user information using BaseUpdateHandler."""

from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.user_repo import PostgresUserRepository
from src.core.events.publisher import EventBus
from src.core.events.publisher import DomainEvent
from dataclasses import dataclass
from typing import List


@dataclass
class UserUpdatedEvent(DomainEvent):
    """Event raised when a user is updated."""
    user_id: int
    updated_fields: List[str]
    updated_by: int
from src.infrastructure.external.password_hasher import PasswordHasher
from src.features.base_update_handler import BaseUpdateHandler
from src.core.decorators import requires_role
from src.core.domain_exceptions import ConflictException, BusinessRuleViolation
from ..models import UpdateUserCommand, User, UserRole, UserCredentials


@dataclass
class UpdateUserResponse:
    id: int
    soeid: str
    role_id: int
    role_name: Optional[str]
    updated_at: datetime


class UpdateUserHandler(BaseUpdateHandler[UpdateUserCommand, UpdateUserResponse, Dict[str, Any]]):
    """Handler for updating user information."""
    
    def __init__(self, uow: PostgresUnitOfWork, user_repo: PostgresUserRepository, password_manager: PasswordHasher = None, event_bus: Optional[EventBus] = None):
        super().__init__(uow)
        self._user_repo = user_repo
        self._password_manager = password_manager or PasswordHasher()
        self._event_bus = event_bus
    
    def get_entity_id(self, command: UpdateUserCommand) -> int:
        """Extract entity ID from command."""
        return command.target_user_id
    
    async def get_entity(self, entity_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve the user to update."""
        return await self._user_repo.get_by_id(entity_id)
    
    def get_entity_name(self) -> str:
        """Get entity name for error messages."""
        return "User"
    
    async def validate_update(self, command: UpdateUserCommand, existing: Dict[str, Any]) -> None:
        """Validate the update operation."""
        # Create domain model from existing data
        user = User.from_repository_data(existing)
        
        # Validate password if provided
        if command.password is not None:
            UserCredentials.validate_password(command.password)
        
        # Check if new soeid is already taken
        if command.soeid is not None:
            existing_user = await self._user_repo.get_by_soeid(command.soeid)
            if existing_user and existing_user['id'] != command.target_user_id:
                raise ConflictException(
                    f"SOEID {command.soeid} is already taken",
                    conflicting_field="soeid",
                    existing_value=command.soeid
                )
            
            # Validate new soeid using domain logic
            try:
                user.update_soeid(command.soeid)
            except BusinessRuleViolation as e:
                raise ConflictException(
                    str(e),
                    conflicting_field="soeid",
                    existing_value=command.soeid
                )
    
    async def prepare_update_data(self, command: UpdateUserCommand, existing: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for update based on command."""
        update_data = {}
        
        if command.soeid is not None:
            update_data['soeid'] = command.soeid
        
        if command.password is not None:
            # Hash the new password
            update_data['password_hash'] = self._password_manager.hash_password(command.password)
        
        if command.role_id is not None:
            update_data['role_id'] = command.role_id
        
        return update_data
    
    async def perform_update(self, entity_id: int, update_data: Dict[str, Any]) -> None:
        """Perform the actual update operation."""
        await self._user_repo.update_user(
            user_id=entity_id,
            **update_data
        )
    
    async def build_response(self, updated_entity: Dict[str, Any]) -> UpdateUserResponse:
        """Build response from updated entity."""
        return UpdateUserResponse(
            id=updated_entity['id'],
            soeid=updated_entity['soeid'],
            role_id=updated_entity['role_id'],
            role_name=updated_entity.get('role_name'),
            updated_at=updated_entity['updated_at']
        )
    
    @requires_role("admin")  # Only admins can update users
    async def handle(self, command: UpdateUserCommand) -> UpdateUserResponse:
        """Update user information."""
        # Call parent's handle method which implements the template
        result = await super().handle(command)
        
        # Publish event after successful update
        if self._event_bus:
            await self._event_bus.publish(UserUpdatedEvent(
                user_id=command.target_user_id,
                updated_fields=list(await self.prepare_update_data(command, await self.get_entity(command.target_user_id))),
                updated_by=command.user_id
            ))
        
        return result