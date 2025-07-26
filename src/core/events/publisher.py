"""Domain events system for cross-feature communication."""

from abc import ABC
from dataclasses import dataclass, field
from typing import Dict, List, Callable, Any, Optional
from datetime import datetime
import asyncio
import logging
from uuid import UUID, uuid4
from enum import Enum

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Standardized event types."""
    # Dataset events
    DATASET_CREATED = "dataset.created"
    DATASET_UPDATED = "dataset.updated"
    DATASET_DELETED = "dataset.deleted"
    
    # Commit events
    COMMIT_CREATED = "commit.created"
    COMMIT_MERGED = "commit.merged"
    
    # User events
    USER_CREATED = "user.created"
    USER_UPDATED = "user.updated"
    USER_DELETED = "user.deleted"
    USER_LOGIN = "user.login"
    
    # Job events
    JOB_CREATED = "job.created"
    JOB_STARTED = "job.started"
    JOB_COMPLETED = "job.completed"
    JOB_FAILED = "job.failed"
    JOB_CANCELLED = "job.cancelled"
    
    # Permission events
    PERMISSION_GRANTED = "permission.granted"
    PERMISSION_REVOKED = "permission.revoked"
    
    # Branch events
    BRANCH_CREATED = "branch.created"
    BRANCH_DELETED = "branch.deleted"
    BRANCH_MERGED = "branch.merged"
    
    # Import events
    IMPORT_STARTED = "import.started"
    IMPORT_COMPLETED = "import.completed"
    IMPORT_FAILED = "import.failed"
    
    # Export events
    EXPORT_REQUESTED = "export.requested"
    EXPORT_COMPLETED = "export.completed"
    EXPORT_FAILED = "export.failed"
    
    # Search events
    SEARCH_INDEX_UPDATED = "search.index.updated"
    SEARCH_INDEX_DELETED = "search.index.deleted"


class DomainEvent(ABC):
    """Base class for all domain events."""
    def __init__(self):
        self.event_id: UUID = uuid4()
        self.occurred_at: datetime = datetime.utcnow()
        self.correlation_id: Optional[UUID] = None
        self.metadata: Dict[str, Any] = {}


# Dataset Events
@dataclass
class DatasetCreatedEvent(DomainEvent):
    """Raised when a new dataset is created."""
    dataset_id: int
    user_id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        super().__init__()


@dataclass
class DatasetUpdatedEvent(DomainEvent):
    """Raised when a dataset is updated."""
    dataset_id: int
    user_id: int
    changes: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        super().__init__()


@dataclass
class DatasetDeletedEvent(DomainEvent):
    """Raised when a dataset is deleted."""
    dataset_id: int
    user_id: int
    name: str
    
    def __post_init__(self):
        super().__init__()


# Permission Events
@dataclass
class PermissionGrantedEvent(DomainEvent):
    """Raised when permission is granted to a user."""
    dataset_id: int
    user_id: int
    target_user_id: int
    permission_type: str
    
    def __post_init__(self):
        super().__init__()


@dataclass
class PermissionRevokedEvent(DomainEvent):
    """Raised when permission is revoked from a user."""
    dataset_id: int
    user_id: int
    target_user_id: int
    permission_type: str
    
    def __post_init__(self):
        super().__init__()


# Job Events
@dataclass
class JobCreatedEvent(DomainEvent):
    """Raised when a new job is created."""
    job_id: UUID
    run_type: str
    user_id: int
    dataset_id: int
    
    def __post_init__(self):
        super().__init__()


@dataclass
class JobCompletedEvent(DomainEvent):
    """Raised when a job completes successfully."""
    job_id: UUID
    status: str
    result: Dict[str, Any] = field(default_factory=dict)
    dataset_id: Optional[int] = None
    
    def __post_init__(self):
        super().__init__()


@dataclass
class JobFailedEvent(DomainEvent):
    """Raised when a job fails."""
    job_id: UUID
    error_message: str
    dataset_id: Optional[int] = None
    
    def __post_init__(self):
        super().__init__()


# Commit Events
@dataclass
class CommitCreatedEvent(DomainEvent):
    """Raised when a new commit is created."""
    commit_id: str
    dataset_id: int
    user_id: int
    message: str
    parent_commit_id: Optional[str] = None
    
    def __post_init__(self):
        super().__init__()
    
    @classmethod
    def from_commit(cls, commit_id: str, dataset_id: int, message: str, user_id: int, parent_commit_id: Optional[str] = None):
        """Create an event from commit details."""
        return cls(
            commit_id=commit_id,
            dataset_id=dataset_id,
            user_id=user_id,
            message=message,
            parent_commit_id=parent_commit_id
        )


# File Events
@dataclass
class FileUploadedEvent(DomainEvent):
    """Raised when a file is uploaded."""
    file_path: str
    dataset_id: int
    user_id: int
    file_size: int
    file_type: str
    
    def __post_init__(self):
        super().__init__()


# Event Handler Type
EventHandler = Callable[[DomainEvent], asyncio.Future]


class EventBus:
    """
    In-memory event bus for publishing and subscribing to domain events.
    
    This is a simple implementation suitable for single-instance applications.
    For distributed systems, consider using a message queue like RabbitMQ or Kafka.
    """
    
    def __init__(self):
        self._handlers: Dict[type, List[EventHandler]] = {}
        self._middleware: List[Callable] = []
    
    def subscribe(self, event_type: type, handler: EventHandler) -> None:
        """
        Subscribe a handler to a specific event type.
        
        Args:
            event_type: The type of event to subscribe to
            handler: Async function that handles the event
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
        logger.debug(f"Handler {handler.__name__} subscribed to {event_type.__name__}")
    
    def unsubscribe(self, event_type: type, handler: EventHandler) -> None:
        """
        Unsubscribe a handler from a specific event type.
        
        Args:
            event_type: The type of event to unsubscribe from
            handler: The handler to remove
        """
        if event_type in self._handlers:
            self._handlers[event_type].remove(handler)
            logger.debug(f"Handler {handler.__name__} unsubscribed from {event_type.__name__}")
    
    async def publish(self, event: DomainEvent) -> None:
        """
        Publish an event to all subscribed handlers.
        
        Args:
            event: The event to publish
        """
        event_type = type(event)
        logger.info(f"Publishing event {event_type.__name__} with ID {event.event_id}")
        
        # Apply middleware
        for middleware in self._middleware:
            event = await middleware(event)
        
        # Get handlers for this event type
        handlers = self._handlers.get(event_type, [])
        
        # Also get handlers for base class (DomainEvent) for generic handlers
        base_handlers = self._handlers.get(DomainEvent, [])
        all_handlers = handlers + base_handlers
        
        if not all_handlers:
            logger.debug(f"No handlers registered for {event_type.__name__}")
            return
        
        # Execute all handlers concurrently
        tasks = [handler(event) for handler in all_handlers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Log any exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                handler_name = all_handlers[i].__name__
                logger.error(
                    f"Handler {handler_name} failed for event {event.event_id}: {result}",
                    exc_info=result
                )
    
    def add_middleware(self, middleware: Callable) -> None:
        """
        Add middleware that processes events before they reach handlers.
        
        Args:
            middleware: Async function that takes an event and returns an event
        """
        self._middleware.append(middleware)
    
    def clear(self) -> None:
        """Clear all handlers and middleware."""
        self._handlers.clear()
        self._middleware.clear()


# Global event bus instance
_event_bus = EventBus()


def get_event_bus() -> EventBus:
    """Get the global event bus instance."""
    return _event_bus


# Decorator for event handlers
def event_handler(event_type: type):
    """
    Decorator to automatically register a function as an event handler.
    
    Usage:
        @event_handler(DatasetCreatedEvent)
        async def handle_dataset_created(event: DatasetCreatedEvent):
            print(f"Dataset {event.dataset_id} created")
    """
    def decorator(func: EventHandler):
        get_event_bus().subscribe(event_type, func)
        return func
    return decorator


# Example middleware for logging all events
async def logging_middleware(event: DomainEvent) -> DomainEvent:
    """Log all events passing through the event bus."""
    logger.debug(
        f"Event: {type(event).__name__} | "
        f"ID: {event.event_id} | "
        f"Time: {event.occurred_at} | "
        f"Data: {event}"
    )
    return event


# Example middleware for adding correlation IDs
async def correlation_middleware(event: DomainEvent) -> DomainEvent:
    """Add correlation ID if not present."""
    if not event.correlation_id:
        event.correlation_id = uuid4()
    return event