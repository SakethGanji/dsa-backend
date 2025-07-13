"""Handler for deleting a branch."""

from typing import Dict
from src.core.abstractions import IUnitOfWork
from src.models.pydantic_models import PermissionType
from src.features.base_handler import BaseHandler, with_error_handling, with_transaction


class DeleteBranchHandler(BaseHandler[Dict[str, str]]):
    """Handler for deleting a branch/ref."""
    
    def __init__(self, uow: IUnitOfWork):
        super().__init__(uow)
    
    @with_transaction
    @with_error_handling
    async def handle(
        self, 
        dataset_id: int, 
        ref_name: str,
        user_id: int
    ) -> Dict[str, str]:
        """
        Delete a branch/ref.
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref name to delete
            user_id: The user ID for permission checking
            
        Returns:
            Dict with success message
        """
        async with self._uow:
            # Check write permission
            has_permission = await self._uow.datasets.check_user_permission(
                dataset_id=dataset_id,
                user_id=user_id,
                required_permission=PermissionType.WRITE.value
            )
            
            if not has_permission:
                # Check if user is admin
                user = await self._uow.users.get_by_id(user_id)
                if not user or user.get('role_name') != 'admin':
                    raise PermissionError(f"User {user_id} does not have permission to delete branches in dataset {dataset_id}")
            
            # Get default branch
            default_branch = await self._uow.commits.get_default_branch(dataset_id)
            
            # Prevent deletion of default branch
            if ref_name == default_branch:
                raise ValueError(f"Cannot delete the default branch '{default_branch}'")
            
            # Delete the ref
            deleted = await self._uow.commits.delete_ref(dataset_id, ref_name)
            
            if not deleted:
                raise ValueError(f"Branch '{ref_name}' not found")
            
            return {"message": f"Branch '{ref_name}' deleted successfully"}