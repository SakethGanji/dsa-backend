"""Base handler for update operations following Template Method pattern."""

from typing import TypeVar, Generic, Optional, Dict, Any, Type
from abc import abstractmethod
from dataclasses import dataclass

from .base_handler import BaseHandler, with_transaction, with_error_handling
from src.core.domain_exceptions import EntityNotFoundException

# Type variables
TCommand = TypeVar('TCommand')
TResponse = TypeVar('TResponse')
TEntity = TypeVar('TEntity')


class BaseUpdateHandler(BaseHandler, Generic[TCommand, TResponse, TEntity]):
    """
    Base handler for all update operations.
    
    Implements Template Method pattern to standardize update flow:
    1. Extract entity ID from command
    2. Validate entity exists
    3. Validate the update (e.g., check unique constraints)
    4. Prepare update data
    5. Perform update
    6. Fetch updated entity
    7. Build response
    """
    
    @abstractmethod
    def get_entity_id(self, command: TCommand) -> Any:
        """
        Extract entity ID from command.
        
        Args:
            command: The update command
            
        Returns:
            The entity ID
        """
        pass
    
    @abstractmethod
    async def get_entity(self, entity_id: Any) -> Optional[TEntity]:
        """
        Retrieve the entity to update.
        
        Args:
            entity_id: The entity ID
            
        Returns:
            The entity if found, None otherwise
        """
        pass
    
    @abstractmethod
    def get_entity_name(self) -> str:
        """
        Get entity name for error messages.
        
        Returns:
            Human-readable entity name (e.g., "Dataset", "User")
        """
        pass
    
    async def validate_update(self, command: TCommand, existing: TEntity) -> None:
        """
        Validate the update operation.
        
        Override this method to add custom validation logic such as:
        - Checking unique constraints
        - Validating business rules
        - Checking permissions beyond basic CRUD
        
        Args:
            command: The update command
            existing: The existing entity
            
        Raises:
            ValueError: If validation fails
        """
        pass  # Default implementation does no additional validation
    
    @abstractmethod
    async def prepare_update_data(self, command: TCommand, existing: TEntity) -> Dict[str, Any]:
        """
        Prepare data for update based on command and existing entity.
        
        This method should:
        - Extract non-None fields from command
        - Transform data if needed
        - Return only fields that should be updated
        
        Args:
            command: The update command
            existing: The existing entity
            
        Returns:
            Dictionary of fields to update
        """
        pass
    
    @abstractmethod
    async def perform_update(self, entity_id: Any, update_data: Dict[str, Any]) -> None:
        """
        Perform the actual update operation.
        
        This method should update the entity in the database.
        It may also handle related updates (e.g., updating tags).
        
        Args:
            entity_id: The entity ID
            update_data: Fields to update
        """
        pass
    
    @abstractmethod
    async def build_response(self, updated_entity: TEntity) -> TResponse:
        """
        Build response from updated entity.
        
        Args:
            updated_entity: The entity after update
            
        Returns:
            The response object
        """
        pass
    
    async def handle_post_update(self, entity_id: Any, command: TCommand, old_entity: TEntity, new_entity: TEntity) -> None:
        """
        Handle any post-update operations.
        
        Override this method to:
        - Publish events
        - Update caches
        - Trigger workflows
        
        Args:
            entity_id: The entity ID
            command: The original command
            old_entity: Entity state before update
            new_entity: Entity state after update
        """
        pass  # Default implementation does nothing
    
    @with_error_handling
    @with_transaction
    async def handle(self, command: TCommand) -> TResponse:
        """
        Template method implementing the update flow.
        
        Args:
            command: The update command
            
        Returns:
            The update response
            
        Raises:
            ValueError: If entity not found or validation fails
        """
        # Store command for potential use in other methods
        self.current_command = command
        
        # 1. Extract entity ID
        entity_id = self.get_entity_id(command)
        
        # 2. Check if entity exists
        existing = await self.get_entity(entity_id)
        if not existing:
            raise EntityNotFoundException(self.get_entity_name(), entity_id)
        
        # 3. Validate the update
        await self.validate_update(command, existing)
        
        # 4. Prepare update data
        update_data = await self.prepare_update_data(command, existing)
        
        # 5. Perform update if there are changes
        if update_data:
            await self.perform_update(entity_id, update_data)
        
        # 6. Fetch updated entity
        updated = await self.get_entity(entity_id)
        if not updated:
            raise EntityNotFoundException(self.get_entity_name(), entity_id)
        
        # 7. Handle post-update operations
        await self.handle_post_update(entity_id, command, existing, updated)
        
        # 8. Build and return response
        return await self.build_response(updated)


class BaseCrudHandler(BaseHandler, Generic[TCommand, TResponse]):
    """
    Base handler for CRUD operations.
    
    Provides a common structure for Create, Read, Update, Delete operations.
    """
    
    @abstractmethod
    async def validate_command(self, command: TCommand) -> None:
        """Validate the command before processing."""
        pass
    
    @abstractmethod
    async def execute_operation(self, command: TCommand) -> Any:
        """Execute the main operation."""
        pass
    
    @abstractmethod
    async def prepare_response(self, result: Any) -> TResponse:
        """Prepare the response from operation result."""
        pass
    
    @with_error_handling
    @with_transaction
    async def handle(self, command: TCommand) -> TResponse:
        """Template method for handling CRUD operations."""
        await self.validate_command(command)
        result = await self.execute_operation(command)
        return await self.prepare_response(result)