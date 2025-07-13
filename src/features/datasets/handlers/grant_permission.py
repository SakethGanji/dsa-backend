from ....core.abstractions import IUnitOfWork, IDatasetRepository
from ....api.models.requests import GrantPermissionRequest
from ....api.models.responses import GrantPermissionResponse
from ....features.base_handler import BaseHandler, with_transaction
from ....core.decorators import requires_permission
from dataclasses import dataclass


@dataclass
class GrantPermissionCommand:
    user_id: int  # Must be first for decorator - this is the granting user
    dataset_id: int
    target_user_id: int
    permission_type: str


class GrantPermissionHandler(BaseHandler):
    """Handler for granting permissions on datasets"""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
    
    @with_transaction
    @requires_permission("datasets", "admin")
    async def handle(
        self,
        command: GrantPermissionCommand
    ) -> GrantPermissionResponse:
        """
        Grant permission to a user on a dataset
        
        Steps:
        1. Verify granting user has admin permission (handled by decorator)
        2. Grant requested permission to target user
        """
        # Transaction and permission check handled by decorators
        
        # Validate permission type
        valid_permissions = ['read', 'write', 'admin']
        if command.permission_type not in valid_permissions:
            raise ValueError(f"Invalid permission type. Must be one of: {valid_permissions}")
        
        # Grant permission
        await self._dataset_repo.grant_permission(
            dataset_id=command.dataset_id,
            user_id=command.target_user_id,
            permission_type=command.permission_type
        )
        
        # Refresh search index to reflect permission changes
        if hasattr(self._uow, 'search_repository'):
            await self._uow.search_repository.refresh_search_index()
        
        return GrantPermissionResponse(
            dataset_id=command.dataset_id,
            user_id=command.target_user_id,
            permission_type=command.permission_type
        )