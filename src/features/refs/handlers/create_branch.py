"""Handler for creating a new branch."""

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.api.models import CreateBranchRequest, CreateBranchResponse, PermissionType
from ...base_handler import BaseHandler, with_error_handling, with_transaction
from src.core.domain_exceptions import EntityNotFoundException, ForbiddenException, ValidationException


class CreateBranchHandler(BaseHandler[CreateBranchResponse]):
    """Handler for creating a new branch from an existing ref."""
    
    def __init__(self, uow: PostgresUnitOfWork):
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
            
            # Use the commit_id from the request directly
            commit_id = request.commit_id.strip()
            
            # Verify the commit exists
            commit_exists = await self._uow.commits.get_commit_by_id(commit_id)
            if not commit_exists:
                raise EntityNotFoundException("Commit", commit_id)
            
            # Verify the commit belongs to this dataset
            if commit_exists.get('dataset_id') != dataset_id:
                raise ValidationException("Commit does not belong to this dataset")
            
            # Create the new ref pointing to the specified commit
            await self._uow.commits.create_ref(
                dataset_id=dataset_id,
                ref_name=request.ref_name,
                commit_id=commit_id
            )
            
            # Get commit details for timestamp
            commit = await self._uow.commits.get_commit_by_id(commit_id)
            
            return CreateBranchResponse(
                dataset_id=dataset_id,
                ref_name=request.ref_name,
                commit_id=commit_id,
                created_at=commit.get('created_at') if commit else None
            )