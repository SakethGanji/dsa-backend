"""Base repository interfaces for common CRUD operations."""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, TypeVar, Generic, List, Union
from uuid import UUID

# Type variables for entity and ID types
TId = TypeVar('TId', int, str, UUID)
TEntity = TypeVar('TEntity', bound=Dict[str, Any])


class IBaseRepository(ABC, Generic[TEntity, TId]):
    """Base repository interface with common CRUD operations."""
    
    @abstractmethod
    async def get_by_id(self, entity_id: TId) -> Optional[TEntity]:
        """
        Retrieve entity by ID.
        
        Args:
            entity_id: The ID of the entity to retrieve
            
        Returns:
            The entity if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def create(self, **kwargs) -> TId:
        """
        Create new entity and return its ID.
        
        Args:
            **kwargs: Entity attributes
            
        Returns:
            The ID of the created entity
        """
        pass
    
    @abstractmethod
    async def update(self, entity_id: TId, **kwargs) -> bool:
        """
        Update entity.
        
        Args:
            entity_id: The ID of the entity to update
            **kwargs: Fields to update
            
        Returns:
            True if updated, False if not found
        """
        pass
    
    @abstractmethod
    async def delete(self, entity_id: TId) -> bool:
        """
        Delete entity.
        
        Args:
            entity_id: The ID of the entity to delete
            
        Returns:
            True if deleted, False if not found
        """
        pass
    
    async def exists(self, entity_id: TId) -> bool:
        """
        Check if entity exists.
        
        Default implementation using get_by_id.
        Subclasses can override for more efficient implementation.
        
        Args:
            entity_id: The ID of the entity to check
            
        Returns:
            True if exists, False otherwise
        """
        entity = await self.get_by_id(entity_id)
        return entity is not None
    
    @abstractmethod
    async def count(self, **filters) -> int:
        """
        Count entities with optional filters.
        
        Args:
            **filters: Filter criteria
            
        Returns:
            Number of entities matching filters
        """
        pass
    
    @abstractmethod
    async def list(
        self, 
        offset: int = 0, 
        limit: int = 100,
        order_by: Optional[str] = None,
        order_desc: bool = False,
        **filters
    ) -> List[TEntity]:
        """
        List entities with pagination and optional filters.
        
        Args:
            offset: Number of entities to skip
            limit: Maximum number of entities to return
            order_by: Field to order by
            order_desc: Whether to order descending
            **filters: Filter criteria
            
        Returns:
            List of entities matching criteria
        """
        pass
    
    async def find_one(self, **filters) -> Optional[TEntity]:
        """
        Find single entity matching filters.
        
        Default implementation using list.
        Subclasses can override for more efficient implementation.
        
        Args:
            **filters: Filter criteria
            
        Returns:
            First entity matching filters, None if not found
        """
        results = await self.list(limit=1, **filters)
        return results[0] if results else None
    
    async def bulk_create(self, entities: List[Dict[str, Any]]) -> List[TId]:
        """
        Create multiple entities.
        
        Default implementation calls create for each entity.
        Subclasses should override for more efficient bulk operations.
        
        Args:
            entities: List of entity data
            
        Returns:
            List of created entity IDs
        """
        ids = []
        for entity_data in entities:
            entity_id = await self.create(**entity_data)
            ids.append(entity_id)
        return ids
    
    async def bulk_delete(self, entity_ids: List[TId]) -> int:
        """
        Delete multiple entities.
        
        Default implementation calls delete for each entity.
        Subclasses should override for more efficient bulk operations.
        
        Args:
            entity_ids: List of entity IDs to delete
            
        Returns:
            Number of entities deleted
        """
        deleted = 0
        for entity_id in entity_ids:
            if await self.delete(entity_id):
                deleted += 1
        return deleted


class IReadOnlyRepository(ABC, Generic[TEntity, TId]):
    """Read-only repository interface for entities that shouldn't be modified."""
    
    @abstractmethod
    async def get_by_id(self, entity_id: TId) -> Optional[TEntity]:
        """Retrieve entity by ID."""
        pass
    
    @abstractmethod
    async def exists(self, entity_id: TId) -> bool:
        """Check if entity exists."""
        pass
    
    @abstractmethod
    async def count(self, **filters) -> int:
        """Count entities with optional filters."""
        pass
    
    @abstractmethod
    async def list(
        self, 
        offset: int = 0, 
        limit: int = 100,
        order_by: Optional[str] = None,
        order_desc: bool = False,
        **filters
    ) -> List[TEntity]:
        """List entities with pagination and optional filters."""
        pass
    
    @abstractmethod
    async def find_one(self, **filters) -> Optional[TEntity]:
        """Find single entity matching filters."""
        pass