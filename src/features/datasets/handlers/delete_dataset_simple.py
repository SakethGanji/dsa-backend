"""Handler for deleting datasets - simplified version."""

from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork, IDatasetRepository
from src.core.domain_exceptions import EntityNotFoundException


@dataclass
class DeleteDatasetCommand:
    user_id: int
    dataset_id: int


@dataclass
class DeleteDatasetResponse:
    """Standardized delete response."""
    entity_type: str = "Dataset"
    entity_id: int = None
    message: str = None


class DeleteDatasetHandler:
    """Handler for deleting datasets."""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository
    ):
        self._uow = uow
        self._dataset_repo = dataset_repo
    
    async def handle(self, command: DeleteDatasetCommand) -> DeleteDatasetResponse:
        """Delete a dataset and all its associated data."""
        # Check if dataset exists
        dataset = await self._dataset_repo.get_dataset_by_id(command.dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Delete the dataset (cascade delete should handle related data)
        await self._dataset_repo.delete_dataset(command.dataset_id)
        
        # Return standardized response
        return DeleteDatasetResponse(
            entity_type="Dataset",
            entity_id=command.dataset_id,
            message=f"Dataset '{dataset['name']}' and all related data have been deleted successfully"
        )