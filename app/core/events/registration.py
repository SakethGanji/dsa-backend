"""Event handler registration system.

This module provides a decorator-based registration system for event handlers,
allowing handlers to be automatically discovered and registered at startup.
"""

from typing import List, Tuple, Callable
from app.core.events.types import EventType, IEventBus

# Registry of event handlers
EVENT_HANDLERS: List[Tuple[EventType, Callable]] = []


def register_handler(event_type: EventType):
    """Decorator to register an event handler.
    
    This decorator adds the decorated function to the global event handler
    registry. The handlers are later registered with the event bus during
    application startup.
    
    Args:
        event_type: The type of event this handler should handle
        
    Example:
        @register_handler(EventType.DATASET_CREATED)
        async def handle_dataset_created(event: Event):
            # Handle the event
            pass
    """
    def decorator(func: Callable) -> Callable:
        EVENT_HANDLERS.append((event_type, func))
        return func
    return decorator


async def setup_event_handlers(event_bus: IEventBus) -> None:
    """Register all event handlers with the event bus.
    
    This function should be called during application startup to register
    all handlers that were decorated with @register_handler.
    
    Args:
        event_bus: The event bus instance to register handlers with
    """
    for event_type, handler in EVENT_HANDLERS:
        await event_bus.subscribe(event_type, handler)