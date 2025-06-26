"""Event-driven architecture interfaces and types.

This module defines the event bus interface and event types for
cross-slice communication using a publish-subscribe pattern.
"""

from typing import Protocol, Dict, Any, Callable, List, Optional
from enum import Enum
from dataclasses import dataclass
from datetime import datetime


class EventType(Enum):
    """Event types for cross-slice communication."""
    
    # Dataset events
    DATASET_CREATED = "dataset.created"
    DATASET_UPDATED = "dataset.updated"
    DATASET_DELETED = "dataset.deleted"
    VERSION_CREATED = "dataset.version.created"
    VERSION_DELETED = "dataset.version.deleted"
    
    # File events
    FILE_UPLOADED = "file.uploaded"
    FILE_DELETED = "file.deleted"
    FILE_DEDUPLICATED = "file.deduplicated"
    
    # Sampling events
    SAMPLE_CREATED = "sample.created"
    SAMPLE_COMPLETED = "sample.completed"
    SAMPLE_FAILED = "sample.failed"
    
    # Permission events (for future use)
    PERMISSION_GRANTED = "permission.granted"
    PERMISSION_REVOKED = "permission.revoked"


@dataclass
class Event:
    """Base event structure for all events in the system."""
    event_type: EventType
    timestamp: datetime
    data: Dict[str, Any]
    correlation_id: Optional[str] = None
    source: Optional[str] = None


class IEventBus(Protocol):
    """Event bus interface for cross-slice communication.
    
    The event bus enables loose coupling between vertical slices
    by providing an asynchronous publish-subscribe mechanism.
    """
    
    async def publish(self, event: Event) -> None:
        """Publish an event to all registered handlers.
        
        Args:
            event: The event to publish
        """
        ...
    
    async def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Subscribe a handler to an event type.
        
        Args:
            event_type: The type of event to subscribe to
            handler: The async function to handle the event
        """
        ...
    
    async def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Unsubscribe a handler from an event type.
        
        Args:
            event_type: The type of event to unsubscribe from
            handler: The handler function to remove
        """
        ...