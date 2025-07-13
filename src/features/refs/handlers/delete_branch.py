"""Handler for deleting a branch with standardized response."""

from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork
from src.api.models import PermissionType
from ...base_handler import BaseHandler, with_transaction
from src.core.decorators import requires_permission
from src.core.domain_exceptions import EntityNotFoundException, BusinessRuleViolation


@dataclass
class DeleteBranchCommand:
    user_id: int  # Must be first for decorator
    dataset_id: int
    ref_name: str


@dataclass
class DeleteBranchResponse:
    """Standardized delete response."""
    entity_type: str = "Branch"
    entity_id: str = None  # Using str since branch names are strings
    success: bool = True
    message: str = None
    
    def __post_init__(self):
        if self.entity_id and not self.message:
            self.message = f"{self.entity_type} '{self.entity_id}' deleted successfully"


class DeleteBranchHandler(BaseHandler):
    """Handler for deleting a branch/ref."""
    
    def __init__(self, uow: IUnitOfWork):
        super().__init__(uow)
    
    @with_transaction
    @requires_permission("datasets", "write")
    async def handle(self, command: DeleteBranchCommand) -> DeleteBranchResponse:
        """
        Delete a branch/ref.
        
        Business Rules:
        - Cannot delete the default branch
        - Branch must exist
        
        Returns:
            DeleteBranchResponse with standardized delete information
        """
        # Get default branch
        default_branch = await self._uow.commits.get_default_branch(command.dataset_id)
        
        # Prevent deletion of default branch
        if command.ref_name == default_branch:
            raise BusinessRuleViolation(
                f"Cannot delete the default branch '{default_branch}'",
                rule="protect_default_branch"
            )
        
        # Delete the ref
        deleted = await self._uow.commits.delete_ref(command.dataset_id, command.ref_name)
        
        if not deleted:
            raise EntityNotFoundException("Branch", command.ref_name)
        
        # Return standardized response
        return DeleteBranchResponse(
            entity_type="Branch",
            entity_id=command.ref_name,
            message=f"Branch '{command.ref_name}' deleted successfully from dataset {command.dataset_id}"
        )