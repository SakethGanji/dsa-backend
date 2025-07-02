"""In-memory event bus implementation.

This module provides a simple in-memory implementation of the event bus
for local development and testing. In production, this could be replaced
with a Redis or message queue based implementation.
"""

from typing import Dict, List, Callable
from collections import defaultdict
import asyncio
import logging

from app.core.events.types import IEventBus, Event, EventType

logger = logging.getLogger(__name__)


class InMemoryEventBus(IEventBus):
    """Simple in-memory event bus implementation.
    
    This implementation uses Python's asyncio for concurrent event handling
    and maintains handlers in memory. All handlers for an event are executed
    concurrently with proper error isolation.
    """
    
    def __init__(self):
        self._handlers: Dict[EventType, List[Callable]] = defaultdict(list)
        self._lock = asyncio.Lock()
    
    async def publish(self, event: Event) -> None:
        """Publish an event to all registered handlers.
        
        Handlers are executed concurrently, and errors in individual
        handlers are logged but do not affect other handlers.
        
        Args:
            event: The event to publish
        """
        handlers = self._handlers.get(event.event_type, [])
        
        if not handlers:
            logger.debug(f"No handlers for event type {event.event_type}")
            return
        
        # Execute handlers concurrently
        tasks = []
        for handler in handlers:
            task = asyncio.create_task(self._execute_handler(handler, event))
            tasks.append(task)
        
        # Wait for all handlers to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Log any handler errors
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(
                    f"Handler {handlers[i].__name__} failed for event {event.event_type}: {result}"
                )
    
    async def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Subscribe a handler to an event type.
        
        Args:
            event_type: The type of event to subscribe to
            handler: The async function to handle the event
        """
        async with self._lock:
            self._handlers[event_type].append(handler)
            logger.info(f"Handler {handler.__name__} subscribed to {event_type}")
    
    async def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Unsubscribe a handler from an event type.
        
        Args:
            event_type: The type of event to unsubscribe from
            handler: The handler function to remove
        """
        async with self._lock:
            if handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)
                logger.info(f"Handler {handler.__name__} unsubscribed from {event_type}")
    
    async def _execute_handler(self, handler: Callable, event: Event) -> None:
        """Execute a single handler with error isolation.
        
        Args:
            handler: The handler function to execute
            event: The event to pass to the handler
            
        Raises:
            Exception: Re-raises handler exceptions to be caught by gather()
        """
        try:
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)
        except Exception as e:
            # Re-raise to be caught by gather()
            raise Exception(f"Handler error: {e}") from e