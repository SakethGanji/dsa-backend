"""Handler for updating dataset information."""

from typing import Optional, Dict, Any
from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork, IDatasetRepository
from src.models.pydantic_models import UpdateDatasetResponse
from src.features.base_handler import BaseHandler, with_transaction
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
    
    @requires_permission("datasets", "write")
    async def handle(self, command: UpdateDatasetCommand) -> UpdateDatasetResponse:
        """
        Update dataset information.
        
        Only non-None fields will be updated.
        """
        # Get existing dataset
        dataset = await self._dataset_repo.get_dataset_by_id(command.dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Prepare update data
        update_data = {}
        if command.name is not None:
            update_data['name'] = command.name
        if command.description is not None:
            update_data['description'] = command.description
        if command.metadata is not None:
            update_data['metadata'] = command.metadata
        
        # Update dataset if there are changes
        if update_data:
            await self._dataset_repo.update_dataset(
                dataset_id=command.dataset_id,
                **update_data
            )
        
        # Update tags if provided
        if command.tags is not None:
            # Remove existing tags
            await self._dataset_repo.remove_dataset_tags(command.dataset_id)
            # Add new tags
            if command.tags:
                await self._dataset_repo.add_dataset_tags(command.dataset_id, command.tags)
        
        # Get updated dataset
        updated_dataset = await self._dataset_repo.get_dataset_by_id(command.dataset_id)
        
        # Get tags
        tags = await self._dataset_repo.get_dataset_tags(command.dataset_id)
        
        return UpdateDatasetResponse(
            dataset_id=updated_dataset['id'],
            name=updated_dataset['name'],
            description=updated_dataset['description'],
            metadata=updated_dataset.get('metadata', {}),
            tags=tags,
            updated_at=updated_dataset['updated_at']
        )