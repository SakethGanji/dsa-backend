"""Handler for updating dataset information - REFACTORED VERSION."""

from typing import Optional, Dict, Any
from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork, IDatasetRepository
from src.api.models import UpdateDatasetResponse
from src.features.base_update_handler import BaseUpdateHandler
from src.core.decorators import requires_permission
from src.core.domain_exceptions import EntityNotFoundException, ErrorMessages, ValidationException
from src.api.factories import ResponseFactory


@dataclass
class UpdateDatasetCommand:
    user_id: int  # Must be first for decorator
    dataset_id: int
    name: Optional[str] = None
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    tags: Optional[list[str]] = None


class UpdateDatasetHandler(BaseUpdateHandler[UpdateDatasetCommand, UpdateDatasetResponse, Dict[str, Any]]):
    """Handler for updating dataset information - using base update handler."""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
    
    def get_entity_id(self, command: UpdateDatasetCommand) -> int:
        """Extract dataset ID from command."""
        return command.dataset_id
    
    def get_entity_name(self) -> str:
        """Return entity name for error messages."""
        return "Dataset"
    
    async def get_entity(self, entity_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve dataset by ID."""
        return await self._dataset_repo.get_dataset_by_id(entity_id)
    
    async def validate_update(self, command: UpdateDatasetCommand, existing: Dict[str, Any]) -> None:
        """
        Validate the update operation.
        
        Could add business rules here, such as:
        - Name uniqueness within organization
        - Permission to change certain fields
        - Valid metadata structure
        """
        # Example: Validate name length if provided
        if command.name is not None and len(command.name) < 3:
            raise ValidationException("Dataset name must be at least 3 characters long", field="name")
    
    async def prepare_update_data(self, command: UpdateDatasetCommand, existing: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for dataset update."""
        update_data = {}
        
        # Only include non-None fields
        if command.name is not None:
            update_data['name'] = command.name
        if command.description is not None:
            update_data['description'] = command.description
        if command.metadata is not None:
            update_data['metadata'] = command.metadata
            
        return update_data
    
    @requires_permission("datasets", "write")
    async def perform_update(self, entity_id: int, update_data: Dict[str, Any]) -> None:
        """Perform the dataset update."""
        # Update main dataset fields
        if update_data:
            await self._dataset_repo.update_dataset(
                dataset_id=entity_id,
                **update_data
            )
        
        # Handle tags separately if provided
        if hasattr(self.current_command, 'tags') and self.current_command.tags is not None:
            await self._dataset_repo.remove_dataset_tags(entity_id)
            if self.current_command.tags:
                await self._dataset_repo.add_dataset_tags(entity_id, self.current_command.tags)
    
    async def build_response(self, updated_entity: Dict[str, Any]) -> UpdateDatasetResponse:
        """Build response from updated dataset."""
        # Get current tags
        tags = await self._dataset_repo.get_dataset_tags(updated_entity['id'])
        
        # Use ResponseFactory for consistent mapping
        return ResponseFactory.from_entity(
            updated_entity,
            UpdateDatasetResponse,
            tags=tags
        )
    
    async def handle_post_update(self, entity_id: int, command: UpdateDatasetCommand, 
                                old_entity: Dict[str, Any], new_entity: Dict[str, Any]) -> None:
        """
        Handle post-update operations.
        
        This could include:
        - Publishing DatasetUpdatedEvent
        - Invalidating caches
        - Sending notifications
        """
        # Example: Publish event if name changed
        if command.name and old_entity.get('name') != new_entity.get('name'):
            # await self._event_bus.publish(DatasetUpdatedEvent(
            #     dataset_id=entity_id,
            #     old_name=old_entity.get('name'),
            #     new_name=new_entity.get('name'),
            #     updated_by=command.user_id
            # ))
            pass