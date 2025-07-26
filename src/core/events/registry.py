"""Event handler registry for managing event-handler mappings."""

from typing import Dict, List, Type, Callable, Awaitable, Any
import logging

from src.core.events.publisher import EventType
from src.core.events.publisher import DomainEvent

# Event handler type
EventHandler = Callable[[DomainEvent], Awaitable[None]]


logger = logging.getLogger(__name__)


class EventHandlerRegistry:
    """Registry for managing event handlers and their mappings."""
    
    def __init__(self):
        self._handlers: Dict[EventType, List[Any]] = {}
        self._handler_instances: Dict[str, Any] = {}
    
    def register_handler(self, handler) -> None:
        """Register an event handler for its declared event types."""
        handler_name = handler.handler_name
        
        if handler_name in self._handler_instances:
            logger.warning(f"Handler {handler_name} already registered, replacing")
        
        self._handler_instances[handler_name] = handler
        
        for event_type in handler.handles():
            if event_type not in self._handlers:
                self._handlers[event_type] = []
            
            self._handlers[event_type].append(handler)
            logger.info(f"Registered {handler_name} for {event_type.value}")
    
    def unregister_handler(self, handler_name: str) -> None:
        """Unregister a handler by name."""
        if handler_name not in self._handler_instances:
            logger.warning(f"Handler {handler_name} not found in registry")
            return
        
        handler = self._handler_instances[handler_name]
        
        for event_type in handler.handles():
            if event_type in self._handlers:
                self._handlers[event_type] = [
                    h for h in self._handlers[event_type] 
                    if h.handler_name != handler_name
                ]
        
        del self._handler_instances[handler_name]
        logger.info(f"Unregistered handler {handler_name}")
    
    def get_handlers(self, event_type: EventType) -> List[Any]:
        """Get all handlers for a specific event type."""
        return self._handlers.get(event_type, [])
    
    def get_all_handlers(self) -> Dict[str, Any]:
        """Get all registered handler instances."""
        return self._handler_instances.copy()
    
    def get_handler_by_name(self, handler_name: str) -> Any:
        """Get a specific handler by name."""
        if handler_name not in self._handler_instances:
            raise ValueError(f"Handler {handler_name} not found")
        return self._handler_instances[handler_name]
    
    def wire_to_event_bus(self, event_bus) -> None:
        """Wire all registered handlers to the event bus."""
        for event_type, handlers in self._handlers.items():
            for handler in handlers:
                # Create async wrapper for the handler
                async def handler_wrapper(event: DomainEvent, h=handler):
                    try:
                        await h.handle(event)
                    except Exception as e:
                        logger.error(
                            f"Handler {h.handler_name} failed for event {event.event_id}: {e}",
                            exc_info=True
                        )
                
                event_bus.subscribe(event_type, handler_wrapper)
                
        logger.info(f"Wired {len(self._handler_instances)} handlers to event bus")
    
    def get_event_handler_mapping(self) -> Dict[str, List[str]]:
        """Get a human-readable mapping of events to handler names."""
        mapping = {}
        for event_type, handlers in self._handlers.items():
            mapping[event_type.value] = [h.handler_name for h in handlers]
        return mapping


class InMemoryEventBus:
    """In-memory implementation of event bus for immediate event handling."""
    
    def __init__(self, store_events: bool = True):
        self._handlers: Dict[EventType, List[EventHandler]] = {}
        self._store_events = store_events
        self._event_store = None
        self._logger = logging.getLogger(self.__class__.__name__)
    
    def set_event_store(self, event_store) -> None:
        """Set the event store for persistence."""
        self._event_store = event_store
    
    async def publish(self, event: DomainEvent) -> None:
        """Publish a domain event to all registered handlers."""
        self._logger.info(
            f"Publishing event {event.event_type.value} for {event.aggregate_type}:{event.aggregate_id}"
        )
        
        # Store event if configured
        if self._store_events and self._event_store:
            try:
                await self._event_store.append(event)
            except Exception as e:
                self._logger.error(f"Failed to store event {event.event_id}: {e}")
                # Continue with handler execution even if storage fails
        
        # Get handlers for this event type
        handlers = self._handlers.get(event.event_type, [])
        
        if not handlers:
            self._logger.warning(f"No handlers registered for {event.event_type.value}")
            return
        
        # Execute handlers
        for handler in handlers:
            try:
                await handler(event)
            except Exception as e:
                self._logger.error(
                    f"Handler failed for event {event.event_id}: {e}",
                    exc_info=True
                )
                # Continue with other handlers
    
    async def publish_batch(self, events: List[DomainEvent]) -> None:
        """Publish multiple events in order."""
        if not events:
            return
        
        # Store all events first if configured
        if self._store_events and self._event_store:
            try:
                await self._event_store.append_batch(events)
            except Exception as e:
                self._logger.error(f"Failed to store event batch: {e}")
                # Continue with handler execution
        
        # Publish each event
        for event in events:
            await self.publish(event)
    
    def subscribe(
        self,
        event_type: EventType,
        handler: EventHandler
    ) -> None:
        """Subscribe a handler to a specific event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        
        self._handlers[event_type].append(handler)
        self._logger.debug(f"Subscribed handler to {event_type.value}")
    
    def unsubscribe(
        self,
        event_type: EventType,
        handler: EventHandler
    ) -> None:
        """Unsubscribe a handler from an event type."""
        if event_type in self._handlers:
            self._handlers[event_type] = [
                h for h in self._handlers[event_type] if h != handler
            ]
    
    def get_handlers(self, event_type: EventType) -> List[EventHandler]:
        """Get all handlers for an event type."""
        return self._handlers.get(event_type, [])