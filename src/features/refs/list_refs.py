"""Handler for listing all refs/branches for a dataset."""

from typing import List, Dict, Any
from src.core.abstractions import IUnitOfWork
from src.models.pydantic_models import ListRefsResponse, RefInfo, PermissionType
from src.features.base_handler import BaseHandler, with_error_handling
from src.core.domain_exceptions import ForbiddenException


class ListRefsHandler(BaseHandler[ListRefsResponse]):
    """Handler for listing all refs/branches for a dataset."""
    
    def __init__(self, uow: IUnitOfWork):
        super().__init__(uow)
    
    @with_error_handling
    async def handle(self, dataset_id: int, user_id: int) -> ListRefsResponse:
        """
        List all refs for a dataset.
        
        Args:
            dataset_id: The dataset ID
            user_id: The user ID for permission checking
            
        Returns:
            ListRefsResponse with all refs
        """
        async with self._uow:
            # Check read permission
            has_permission = await self._uow.datasets.check_user_permission(
                dataset_id=dataset_id,
                user_id=user_id,
                required_permission=PermissionType.READ.value
            )
            
            if not has_permission:
                # Check if user is admin
                user = await self._uow.users.get_by_id(user_id)
                if not user or user.get('role_name') != 'admin':
                    raise ForbiddenException()
            
            # Get all refs for the dataset
            refs = await self._uow.commits.list_refs(dataset_id)
            
            # Convert to response model
            ref_infos = [
                RefInfo(
                    ref_name=ref['name'],
                    commit_id=ref['commit_id'],
                    is_default=ref.get('name') == 'main',  # Assuming 'main' is default
                    created_at=ref['created_at'],
                    updated_at=ref['updated_at']
                )
                for ref in refs
            ]
            
            return ListRefsResponse(
                refs=ref_infos,
                dataset_id=dataset_id
            )