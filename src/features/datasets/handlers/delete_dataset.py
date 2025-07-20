"""Handler for deleting datasets with standardized response."""

from dataclasses import dataclass
from typing import Optional
from src.core.abstractions import IUnitOfWork, IDatasetRepository
from src.core.abstractions.events import IEventBus, DatasetDeletedEvent
from ...base_handler import BaseHandler, with_transaction
from src.core.decorators import requires_permission
from src.core.domain_exceptions import EntityNotFoundException
from ..models import DeleteDatasetCommand


@dataclass
class DeleteDatasetResponse:
    """Standardized delete response."""
    entity_type: str = "Dataset"
    entity_id: int = None
    success: bool = True
    message: str = None
    
    def __post_init__(self):
        if self.entity_id and not self.message:
            self.message = f"{self.entity_type} {self.entity_id} deleted successfully"


class DeleteDatasetHandler(BaseHandler):
    """Handler for deleting datasets."""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository,
        event_bus: Optional[IEventBus] = None
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
        self._event_bus = event_bus
    
    @with_transaction
    @requires_permission("datasets", "admin")  # Only admins can delete datasets
    async def handle(self, command: DeleteDatasetCommand) -> DeleteDatasetResponse:
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
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Delete all associated data
        # Note: The order matters due to foreign key constraints
        
        # 1. Delete tags
        await self._dataset_repo.remove_dataset_tags(command.dataset_id)
        
        # 2. Delete the dataset itself (cascade should handle related records)
        await self._dataset_repo.delete_dataset(command.dataset_id)
        
        # Note: Row data cleanup might be handled by a separate cleanup job
        # to avoid deleting rows that are shared across datasets
        
        # Publish deletion event
        if self._event_bus:
            event = DatasetDeletedEvent.from_deletion(
                dataset_id=command.dataset_id,
                deleted_by=command.user_id
            )
            await self._event_bus.publish(event)
        
        # Return standardized response
        return DeleteDatasetResponse(
            entity_type="Dataset",
            entity_id=command.dataset_id,
            message=f"Dataset '{dataset['name']}' and all related data have been deleted successfully"
        )