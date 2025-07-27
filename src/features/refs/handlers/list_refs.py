"""Handler for listing all refs/branches for a dataset."""

from typing import List, Dict, Any
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.api.models import ListRefsResponse, RefInfo
from ...base_handler import BaseHandler, with_error_handling
from src.core.permissions import PermissionService
from src.core.domain_exceptions import ForbiddenException


class ListRefsHandler(BaseHandler[ListRefsResponse]):
    """Handler for listing all refs/branches for a dataset."""
    
    def __init__(self, uow: PostgresUnitOfWork, permissions: PermissionService):
        super().__init__(uow)
        self._permissions = permissions
    
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
            await self._permissions.require("dataset", dataset_id, user_id, "read")
            
            # Get all refs for the dataset
            refs = await self._uow.commits.list_refs(dataset_id)
            
            # Convert to response model
            ref_infos = [
                RefInfo(
                    ref_name=ref['name'],
                    commit_id=ref['commit_id'].strip() if ref['commit_id'] else '',  # Trim whitespace
                    dataset_id=dataset_id,
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