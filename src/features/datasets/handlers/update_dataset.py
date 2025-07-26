"""Handler for updating dataset information."""

from typing import Optional, Dict, Any
from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork, IDatasetRepository
from src.core.abstractions.events import IEventBus, DatasetUpdatedEvent
from src.api.models import UpdateDatasetResponse
from ...base_update_handler import BaseUpdateHandler
from src.core.decorators import requires_permission
from src.core.domain_exceptions import EntityNotFoundException, ErrorMessages, ValidationException
from src.api.factories import ResponseFactory
from ..models import UpdateDatasetCommand


class UpdateDatasetHandler(BaseUpdateHandler[UpdateDatasetCommand, UpdateDatasetResponse, Dict[str, Any]]):
    """Handler for updating dataset information - using base update handler."""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository,
        event_bus: Optional[IEventBus] = None
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
        self._event_bus = event_bus
    
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
        
        Checks:
        - User permissions
        - Name uniqueness
        - Valid field values
        """
        # Check user has write permission
        has_permission = await self._dataset_repo.check_user_permission(
            command.dataset_id, command.user_id, "write"
        )
        if not has_permission:
            from src.core.domain_exceptions import PermissionDeniedError
            raise PermissionDeniedError(
                f"User {command.user_id} does not have write permission on dataset {command.dataset_id}"
            )
        
        # Validate name length if provided
        if command.name is not None and len(command.name) < 3:
            raise ValidationException("Dataset name must be at least 3 characters long", field="name")
        
        # Note: In a full implementation, we would check for unique names here
    
    async def prepare_update_data(self, command: UpdateDatasetCommand, existing: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for dataset update."""
        update_data = {}
        
        # Only include non-None fields
        if command.name is not None:
            update_data['name'] = command.name
        if command.description is not None:
            update_data['description'] = command.description
            
        return update_data
    
    async def perform_update(self, entity_id: int, update_data: Dict[str, Any]) -> None:
        """Perform the dataset update."""
        # Update main dataset fields
        if update_data:
            await self._dataset_repo.update_dataset(
                dataset_id=entity_id,
                **update_data
            )
        
        # Tags are handled in handle_post_update through the command parameter
    
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
        """Handle post-update operations including tags and events."""
        # Handle tags update
        if command.tags is not None:
            await self._dataset_repo.remove_dataset_tags(entity_id)
            if command.tags:
                await self._dataset_repo.add_dataset_tags(entity_id, command.tags)
        
        # Publish event if event bus is available
        if not self._event_bus:
            return
        
        # Determine what changed
        changes = {}
        if command.name is not None and old_entity.get('name') != command.name:
            changes['name'] = {'old': old_entity.get('name'), 'new': command.name}
        if command.description is not None and old_entity.get('description') != command.description:
            changes['description'] = {'old': old_entity.get('description'), 'new': command.description}
        if command.tags is not None:
            old_tags = await self._dataset_repo.get_dataset_tags(entity_id)
            if set(old_tags) != set(command.tags):
                changes['tags'] = {'old': old_tags, 'new': command.tags}
        
        # Publish event if there were changes
        if changes:
            event = DatasetUpdatedEvent.from_update(
                dataset_id=entity_id,
                changes=changes,
                updated_by=command.user_id
            )
            await self._event_bus.publish(event)