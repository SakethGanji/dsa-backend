"""Event handlers for search indexing."""

import logging
from typing import List

from src.core.abstractions.events import (
    IEventHandler, DomainEvent, EventType,
    DatasetCreatedEvent, DatasetUpdatedEvent, DatasetDeletedEvent
)
from src.infrastructure.postgres.database import DatabasePool


logger = logging.getLogger(__name__)


class SearchIndexEventHandler(IEventHandler):
    """Handler for updating search indexes based on dataset events."""
    
    def __init__(self, db_pool: DatabasePool):
        self._db_pool = db_pool
    
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
        
        async with self._db_pool.acquire() as conn:
            # Update search_datasets materialized view
            await conn.execute("""
                -- Refresh materialized view to include new dataset
                REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_core.search_datasets
            """)
            
            logger.info(f"Added dataset {dataset_id} to search index")
    
    async def _handle_dataset_updated(self, event: DatasetUpdatedEvent) -> None:
        """Handle dataset update by refreshing search index."""
        dataset_id = int(event.aggregate_id)
        changes = event.payload.get('changes', {})
        
        # Only refresh if searchable fields changed
        searchable_fields = {'name', 'description', 'tags'}
        if any(field in changes for field in searchable_fields):
            async with self._db_pool.acquire() as conn:
                await conn.execute("""
                    -- Refresh materialized view to reflect updates
                    REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_core.search_datasets
                """)
                
                logger.info(f"Updated dataset {dataset_id} in search index")
    
    async def _handle_dataset_deleted(self, event: DatasetDeletedEvent) -> None:
        """Handle dataset deletion by removing from search index."""
        dataset_id = int(event.aggregate_id)
        
        async with self._db_pool.acquire() as conn:
            # The materialized view will automatically exclude deleted datasets
            # on next refresh since they won't exist in the source tables
            await conn.execute("""
                -- Refresh materialized view to remove deleted dataset
                REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_core.search_datasets
            """)
            
            logger.info(f"Removed dataset {dataset_id} from search index")
    
    @property
    def handler_name(self) -> str:
        """Return the name of this handler for logging."""
        return "SearchIndexEventHandler"