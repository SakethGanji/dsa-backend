"""Handler for updating dataset information - simplified version."""

from typing import Optional, Dict, Any
from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork, IDatasetRepository
from src.api.models import UpdateDatasetResponse
from ...base_handler import BaseHandler, with_transaction
from src.core.decorators import requires_permission
from src.core.domain_exceptions import EntityNotFoundException


@dataclass
class UpdateDatasetCommand:
    user_id: int  # Must be first for decorator
    dataset_id: int
    name: Optional[str] = None
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    tags: Optional[list[str]] = None


class UpdateDatasetHandler(BaseHandler):
    """Handler for updating dataset information."""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
    
    async def handle(self, command: UpdateDatasetCommand) -> UpdateDatasetResponse:
        """Update dataset information."""
        # Check if dataset exists
        dataset = await self._dataset_repo.get_dataset_by_id(command.dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Update name and description if provided
        if command.name is not None or command.description is not None:
            await self._dataset_repo.update_dataset(
                dataset_id=command.dataset_id,
                name=command.name,
                description=command.description
            )
        
        # Update tags if provided
        if command.tags is not None:
            # Remove all existing tags and add new ones
            await self._dataset_repo.remove_dataset_tags(command.dataset_id)
            if command.tags:
                await self._dataset_repo.add_dataset_tags(command.dataset_id, command.tags)
        
        # Get updated dataset info
        updated_dataset = await self._dataset_repo.get_dataset_by_id(command.dataset_id)
        tags = await self._dataset_repo.get_dataset_tags(command.dataset_id)
        
        return UpdateDatasetResponse(
            dataset_id=command.dataset_id,
            name=updated_dataset['name'],
            description=updated_dataset['description'],
            metadata={},  # No metadata column in database
            tags=tags,
            updated_at=updated_dataset['updated_at']
        )