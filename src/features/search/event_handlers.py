"""Event handlers for updating search index based on domain events."""

import logging
from src.core.events import (
    event_handler,
    DatasetCreatedEvent,
    DatasetUpdatedEvent,
    DatasetDeletedEvent,
    PermissionGrantedEvent,
    PermissionRevokedEvent
)
from src.core.abstractions import IUnitOfWork

logger = logging.getLogger(__name__)


class SearchIndexEventHandler:
    """Handles domain events to keep search index updated."""
    
    def __init__(self, uow: IUnitOfWork):
        self._uow = uow
    
    async def handle_dataset_created(self, event: DatasetCreatedEvent) -> None:
        """Update search index when a dataset is created."""
        logger.info(f"Updating search index for new dataset {event.dataset_id}")
        
        async with self._uow:
            if hasattr(self._uow, 'search_repository'):
                await self._uow.search_repository.refresh_search_index()
    
    async def handle_dataset_updated(self, event: DatasetUpdatedEvent) -> None:
        """Update search index when a dataset is updated."""
        logger.info(f"Updating search index for updated dataset {event.dataset_id}")
        
        async with self._uow:
            if hasattr(self._uow, 'search_repository'):
                await self._uow.search_repository.refresh_search_index()
    
    async def handle_dataset_deleted(self, event: DatasetDeletedEvent) -> None:
        """Update search index when a dataset is deleted."""
        logger.info(f"Updating search index after deleting dataset {event.dataset_id}")
        
        async with self._uow:
            if hasattr(self._uow, 'search_repository'):
                await self._uow.search_repository.refresh_search_index()
    
    async def handle_permission_changed(self, event: PermissionGrantedEvent | PermissionRevokedEvent) -> None:
        """Update search index when permissions change."""
        logger.info(f"Updating search index after permission change for dataset {event.dataset_id}")
        
        async with self._uow:
            if hasattr(self._uow, 'search_repository'):
                await self._uow.search_repository.refresh_search_index()


# Example of using the decorator pattern
@event_handler(DatasetCreatedEvent)
async def log_dataset_creation(event: DatasetCreatedEvent) -> None:
    """Simple event handler that logs dataset creation."""
    logger.info(
        f"Dataset created: ID={event.dataset_id}, "
        f"Name={event.name}, "
        f"User={event.user_id}, "
        f"Tags={event.tags}"
    )


@event_handler(DatasetDeletedEvent)
async def log_dataset_deletion(event: DatasetDeletedEvent) -> None:
    """Simple event handler that logs dataset deletion."""
    logger.warning(
        f"Dataset deleted: ID={event.dataset_id}, "
        f"Name={event.name}, "
        f"User={event.user_id}"
    )


def register_search_event_handlers(event_bus, uow: IUnitOfWork) -> None:
    """Register all search-related event handlers."""
    handler = SearchIndexEventHandler(uow)
    
    # Register handlers
    event_bus.subscribe(DatasetCreatedEvent, handler.handle_dataset_created)
    event_bus.subscribe(DatasetUpdatedEvent, handler.handle_dataset_updated)
    event_bus.subscribe(DatasetDeletedEvent, handler.handle_dataset_deleted)
    event_bus.subscribe(PermissionGrantedEvent, handler.handle_permission_changed)
    event_bus.subscribe(PermissionRevokedEvent, handler.handle_permission_changed)
    
    logger.info("Search event handlers registered")