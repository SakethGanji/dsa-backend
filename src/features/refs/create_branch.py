"""Handler for creating a new branch."""

from src.core.abstractions import IUnitOfWork
from src.models.pydantic_models import CreateBranchRequest, CreateBranchResponse, PermissionType
from src.features.base_handler import BaseHandler, with_error_handling, with_transaction
from src.core.domain_exceptions import EntityNotFoundException, ForbiddenException


class CreateBranchHandler(BaseHandler[CreateBranchResponse]):
    """Handler for creating a new branch from an existing ref."""
    
    def __init__(self, uow: IUnitOfWork):
        super().__init__(uow)
    
    @with_transaction
    @with_error_handling
    async def handle(
        self, 
        dataset_id: int, 
        request: CreateBranchRequest, 
        user_id: int
    ) -> CreateBranchResponse:
        """
        Create a new branch from an existing ref.
        
        Args:
            dataset_id: The dataset ID
            request: The branch creation request
            user_id: The user ID for permission checking
            
        Returns:
            CreateBranchResponse with branch details
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
                    raise ForbiddenException()
            
            # Get the commit ID from the source ref
            source_commit_id = await self._uow.commits.get_current_commit_for_ref(
                dataset_id, request.from_ref
            )
            
            if not source_commit_id:
                raise EntityNotFoundException("Ref", request.from_ref)
            
            # Create the new ref pointing to the same commit
            await self._uow.commits.create_ref(
                dataset_id=dataset_id,
                ref_name=request.name,
                commit_id=source_commit_id
            )
            
            return CreateBranchResponse(
                name=request.name,
                commit_id=source_commit_id,
                created_from=request.from_ref
            )