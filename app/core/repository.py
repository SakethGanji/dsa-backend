"""Core repository interfaces and base implementations.

This module provides generic repository patterns that can be extended
by specific vertical slices for their data access needs.
"""
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional, List, Dict, Any, Type
from datetime import datetime
from dataclasses import dataclass


T = TypeVar('T')  # Generic type for entities
ID = TypeVar('ID')  # Generic type for entity IDs


@dataclass
class PaginationParams:
    """Standard pagination parameters."""
    offset: int = 0
    limit: int = 20
    sort_by: Optional[str] = None
    sort_order: str = "desc"


@dataclass
class PaginatedResult(Generic[T]):
    """Container for paginated results."""
    items: List[T]
    total: int
    offset: int
    limit: int
    
    @property
    def has_next(self) -> bool:
        """Check if there are more results."""
        return self.offset + self.limit < self.total
    
    @property
    def has_previous(self) -> bool:
        """Check if there are previous results."""
        return self.offset > 0


class IRepository(ABC, Generic[T, ID]):
    """Base repository interface for CRUD operations.
    
    This interface defines the standard data access operations that
    should be available for any entity type.
    """
    
    @abstractmethod
    async def create(self, entity: T) -> T:
        """Create a new entity.
        
        Args:
            entity: The entity to create
            
        Returns:
            The created entity with generated ID
        """
        pass
    
    @abstractmethod
    async def get_by_id(self, entity_id: ID) -> Optional[T]:
        """Get an entity by its ID.
        
        Args:
            entity_id: The ID of the entity to retrieve
            
        Returns:
            The entity if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def update(self, entity_id: ID, updates: Dict[str, Any]) -> Optional[T]:
        """Update an entity with the given changes.
        
        Args:
            entity_id: The ID of the entity to update
            updates: Dictionary of field updates
            
        Returns:
            The updated entity if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def delete(self, entity_id: ID) -> bool:
        """Delete an entity by ID.
        
        Args:
            entity_id: The ID of the entity to delete
            
        Returns:
            True if deleted, False if not found
        """
        pass
    
    @abstractmethod
    async def exists(self, entity_id: ID) -> bool:
        """Check if an entity exists.
        
        Args:
            entity_id: The ID to check
            
        Returns:
            True if exists, False otherwise
        """
        pass


class IPaginatedRepository(IRepository[T, ID], ABC):
    """Repository interface with pagination support."""
    
    @abstractmethod
    async def list(
        self,
        params: PaginationParams,
        filters: Optional[Dict[str, Any]] = None
    ) -> PaginatedResult[T]:
        """List entities with pagination and optional filters.
        
        Args:
            params: Pagination parameters
            filters: Optional filters to apply
            
        Returns:
            Paginated result containing the entities
        """
        pass
    
    @abstractmethod
    async def count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """Count entities matching the filters.
        
        Args:
            filters: Optional filters to apply
            
        Returns:
            Total count of matching entities
        """
        pass


class ISoftDeleteRepository(IPaginatedRepository[T, ID], ABC):
    """Repository interface with soft delete support."""
    
    @abstractmethod
    async def soft_delete(self, entity_id: ID) -> Optional[T]:
        """Soft delete an entity (mark as deleted without removing).
        
        Args:
            entity_id: The ID of the entity to soft delete
            
        Returns:
            The soft deleted entity if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def restore(self, entity_id: ID) -> Optional[T]:
        """Restore a soft deleted entity.
        
        Args:
            entity_id: The ID of the entity to restore
            
        Returns:
            The restored entity if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def list_deleted(self, params: PaginationParams) -> PaginatedResult[T]:
        """List soft deleted entities.
        
        Args:
            params: Pagination parameters
            
        Returns:
            Paginated result of deleted entities
        """
        pass
    
    @abstractmethod
    async def purge(self, entity_id: ID) -> bool:
        """Permanently delete a soft deleted entity.
        
        Args:
            entity_id: The ID of the entity to purge
            
        Returns:
            True if purged, False if not found
        """
        pass


class ITaggableRepository(ABC, Generic[T, ID]):
    """Repository interface for entities that support tagging."""
    
    @abstractmethod
    async def add_tag(self, entity_id: ID, tag: str) -> None:
        """Add a tag to an entity.
        
        Args:
            entity_id: The ID of the entity
            tag: The tag to add
        """
        pass
    
    @abstractmethod
    async def remove_tag(self, entity_id: ID, tag: str) -> None:
        """Remove a tag from an entity.
        
        Args:
            entity_id: The ID of the entity
            tag: The tag to remove
        """
        pass
    
    @abstractmethod
    async def get_tags(self, entity_id: ID) -> List[str]:
        """Get all tags for an entity.
        
        Args:
            entity_id: The ID of the entity
            
        Returns:
            List of tags
        """
        pass
    
    @abstractmethod
    async def find_by_tags(
        self,
        tags: List[str],
        match_all: bool = False,
        params: Optional[PaginationParams] = None
    ) -> PaginatedResult[T]:
        """Find entities by tags.
        
        Args:
            tags: List of tags to search for
            match_all: If True, match all tags; if False, match any tag
            params: Optional pagination parameters
            
        Returns:
            Paginated result of matching entities
        """
        pass


class IVersionedRepository(ABC, Generic[T, ID]):
    """Repository interface for entities that support versioning."""
    
    @abstractmethod
    async def create_version(
        self,
        entity_id: ID,
        version_data: Dict[str, Any],
        message: Optional[str] = None
    ) -> T:
        """Create a new version of an entity.
        
        Args:
            entity_id: The ID of the entity
            version_data: Data for the new version
            message: Optional version message
            
        Returns:
            The new version
        """
        pass
    
    @abstractmethod
    async def get_version(self, entity_id: ID, version_number: int) -> Optional[T]:
        """Get a specific version of an entity.
        
        Args:
            entity_id: The ID of the entity
            version_number: The version number
            
        Returns:
            The version if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def list_versions(
        self,
        entity_id: ID,
        params: Optional[PaginationParams] = None
    ) -> PaginatedResult[T]:
        """List all versions of an entity.
        
        Args:
            entity_id: The ID of the entity
            params: Optional pagination parameters
            
        Returns:
            Paginated result of versions
        """
        pass
    
    @abstractmethod
    async def get_latest_version(self, entity_id: ID) -> Optional[T]:
        """Get the latest version of an entity.
        
        Args:
            entity_id: The ID of the entity
            
        Returns:
            The latest version if found, None otherwise
        """
        pass