"""Event handlers for search indexing."""

import logging
from typing import List, Optional

from src.core.events.publisher import (
    DomainEvent,
    DatasetCreatedEvent, DatasetUpdatedEvent, DatasetDeletedEvent
)
from src.core.events.publisher import EventType
from src.infrastructure.postgres.database import DatabasePool
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from .services import SearchService


logger = logging.getLogger(__name__)


class SearchIndexEventHandler:
    """Handler for updating search indexes based on dataset events."""
    
    def __init__(self, db_pool: DatabasePool):
        self._db_pool = db_pool
        self._search_service: Optional[SearchService] = None
    
    async def _get_service(self) -> SearchService:
        """Get or create the search service instance."""
        if not self._search_service:
            uow = PostgresUnitOfWork(self._db_pool)
            self._search_service = SearchService(uow)
        return self._search_service
    
    def handles(self) -> List[EventType]:
        """Return list of event types this handler processes."""
        return [
            EventType.DATASET_CREATED,
            EventType.DATASET_UPDATED,
            EventType.DATASET_DELETED
        ]
    
    async def handle(self, event: DomainEvent) -> None:
        """Handle dataset events by updating search indexes."""
        logger.info(f"SearchIndexEventHandler processing {event.event_type.value}")
        
        if event.event_type == EventType.DATASET_CREATED:
            await self._handle_dataset_created(event)
        elif event.event_type == EventType.DATASET_UPDATED:
            await self._handle_dataset_updated(event)
        elif event.event_type == EventType.DATASET_DELETED:
            await self._handle_dataset_deleted(event)
    
    async def _handle_dataset_created(self, event: DatasetCreatedEvent) -> None:
        """Handle dataset creation by adding to search index."""
        dataset_id = int(event.aggregate_id)
        
        # Use service to handle the update
        service = await self._get_service()
        await service.handle_dataset_created(dataset_id)
        
        logger.info(f"Added dataset {dataset_id} to search index")
    
    async def _handle_dataset_updated(self, event: DatasetUpdatedEvent) -> None:
        """Handle dataset update by refreshing search index."""
        dataset_id = int(event.aggregate_id)
        changes = event.payload.get('changes', {})
        
        # Use service to handle the update
        service = await self._get_service()
        await service.handle_dataset_updated(dataset_id, changes)
        
        # Only log if searchable fields changed
        searchable_fields = {'name', 'description', 'tags'}
        if any(field in changes for field in searchable_fields):
            logger.info(f"Updated dataset {dataset_id} in search index")
    
    async def _handle_dataset_deleted(self, event: DatasetDeletedEvent) -> None:
        """Handle dataset deletion by removing from search index."""
        dataset_id = int(event.aggregate_id)
        
        # Use service to handle the deletion
        service = await self._get_service()
        await service.handle_dataset_deleted(dataset_id)
        
        logger.info(f"Removed dataset {dataset_id} from search index")
    
    @property
    def handler_name(self) -> str:
        """Return the name of this handler for logging."""
        return "SearchIndexEventHandler"