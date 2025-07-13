"""Handler for deleting users."""

from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork, IUserRepository
from src.features.base_handler import BaseHandler, with_transaction
from src.core.decorators import requires_role
from src.core.domain_exceptions import EntityNotFoundException, BusinessRuleViolation


@dataclass
class DeleteUserCommand:
    user_id: int  # Must be first for decorator - this is the requesting user (admin)
    target_user_id: int


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
    
    def __init__(self, uow: IUnitOfWork, user_repo: IUserRepository):
        super().__init__(uow)
        self._user_repo = user_repo
    
    @with_transaction
    @requires_role("admin")  # Only admins can delete users
    async def handle(self, command: DeleteUserCommand) -> DeleteUserResponse:
        """
        Delete a user and handle related cleanup.
        
        Note: This may leave orphaned data if the user created datasets.
        Consider implementing soft delete or ownership transfer instead.
        """
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
        
        # Return standardized response
        return DeleteUserResponse(
            entity_type="User",
            entity_id=command.target_user_id,
            message=f"User '{user['soeid']}' has been deleted successfully"
        )