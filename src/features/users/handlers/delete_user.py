"""Handler for deleting users."""

from dataclasses import dataclass
from typing import Optional
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.user_repo import PostgresUserRepository
from src.core.events.publisher import EventBus
from src.core.events.publisher import DomainEvent
from dataclasses import dataclass


@dataclass
class UserDeletedEvent(DomainEvent):
    """Event raised when a user is deleted."""
    user_id: int
    deleted_by: int
    user_soeid: str
from ...base_handler import BaseHandler, with_transaction
from src.core.permissions import PermissionService
from src.core.domain_exceptions import EntityNotFoundException, BusinessRuleViolation
from ..models.commands import DeleteUserCommand


@dataclass
class DeleteUserResponse:
    """Standardized delete response."""
    entity_type: str = "User"
    entity_id: int = None
    success: bool = True
    message: str = None
    
    def __post_init__(self):
        if self.entity_id and not self.message:
            self.message = f"{self.entity_type} {self.entity_id} deleted successfully"


class DeleteUserHandler(BaseHandler):
    """Handler for deleting users."""
    
    def __init__(self, uow: PostgresUnitOfWork, user_repo: PostgresUserRepository, permissions: PermissionService, event_bus: Optional[EventBus] = None):
        super().__init__(uow)
        self._user_repo = user_repo
        self._permissions = permissions
        self._event_bus = event_bus
    
    @with_transaction
    async def handle(self, command: DeleteUserCommand) -> DeleteUserResponse:
        """
        Delete a user and handle related cleanup.
        
        Note: This may leave orphaned data if the user created datasets.
        Consider implementing soft delete or ownership transfer instead.
        """
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
            admin_count = await self._user_repo.count_users_by_role('admin')
            if admin_count <= 1:
                raise BusinessRuleViolation("Cannot delete the last admin user", rule="maintain_admin_count")
        
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