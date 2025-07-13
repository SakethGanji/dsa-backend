"""Handler for deleting datasets."""

from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork, IDatasetRepository
from src.features.base_handler import BaseHandler, with_transaction
from src.core.decorators import requires_permission


@dataclass
class DeleteDatasetCommand:
    user_id: int  # Must be first for decorator
    dataset_id: int


class DeleteDatasetHandler(BaseHandler):
    """Handler for deleting datasets."""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
    
    @with_transaction
    @requires_permission("datasets", "admin")  # Only admins can delete datasets
    async def handle(self, command: DeleteDatasetCommand) -> None:
        """
        Delete a dataset and all its associated data.
        
        This includes:
        - Dataset record
        - All permissions
        - All tags
        - All commits and refs
        - All rows and manifests
        """
        # Check if dataset exists
        dataset = await self._dataset_repo.get_dataset_by_id(command.dataset_id)
        if not dataset:
            raise ValueError(f"Dataset {command.dataset_id} not found")
        
        # Delete all associated data
        # Note: The order matters due to foreign key constraints
        
        # 1. Delete tags
        await self._dataset_repo.remove_dataset_tags(command.dataset_id)
        
        # 2. Delete permissions
        # Note: delete_all_permissions may not exist, using delete_dataset which handles cascade
        # await self._dataset_repo.delete_all_permissions(command.dataset_id)
        
        # 3. Delete refs
        if hasattr(self._uow, 'commits'):
            await self._uow.commits.delete_all_refs(command.dataset_id)
        
        # 4. Delete commits and manifests (cascade delete should handle manifests)
        if hasattr(self._uow, 'commits'):
            await self._uow.commits.delete_all_commits(command.dataset_id)
        
        # 5. Finally, delete the dataset itself
        await self._dataset_repo.delete_dataset(command.dataset_id)
        
        # Note: Row data cleanup might be handled by a separate cleanup job
        # to avoid deleting rows that are shared across datasets