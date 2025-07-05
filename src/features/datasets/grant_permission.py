from ...core.services.interfaces import IUnitOfWork, IDatasetRepository
from ...models.pydantic_models import GrantPermissionRequest, GrantPermissionResponse


class GrantPermissionHandler:
    """Handler for granting permissions on datasets"""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository
    ):
        self._uow = uow
        self._dataset_repo = dataset_repo
    
    async def handle(
        self,
        dataset_id: int,
        request: GrantPermissionRequest,
        granting_user_id: int
    ) -> GrantPermissionResponse:
        """
        Grant permission to a user on a dataset
        
        Steps:
        1. Verify granting user has admin permission
        2. Grant requested permission to target user
        """
        # TODO: Check if granting user has admin permission
        has_admin = await self._dataset_repo.check_user_permission(
            dataset_id, granting_user_id, 'admin'
        )
        if not has_admin:
            raise PermissionError("Only admins can grant permissions")
        
        # TODO: Validate permission type
        valid_permissions = ['read', 'write', 'admin']
        if request.permission_type not in valid_permissions:
            raise ValueError(f"Invalid permission type. Must be one of: {valid_permissions}")
        
        await self._uow.begin()
        try:
            # Grant permission
            await self._dataset_repo.grant_permission(
                dataset_id=dataset_id,
                user_id=request.user_id,
                permission_type=request.permission_type
            )
            
            await self._uow.commit()
            
            return GrantPermissionResponse(
                dataset_id=dataset_id,
                user_id=request.user_id,
                permission_type=request.permission_type
            )
        except Exception:
            await self._uow.rollback()
            raise