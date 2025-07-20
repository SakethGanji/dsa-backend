"""Event-driven architecture abstractions."""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Type, Callable, Awaitable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import uuid


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
    
    # Import events
    IMPORT_STARTED = "import.started"
    IMPORT_COMPLETED = "import.completed"
    IMPORT_FAILED = "import.failed"
    
    # Analysis events
    ANALYSIS_STARTED = "analysis.started"
    ANALYSIS_COMPLETED = "analysis.completed"
    
    # Export events
    EXPORT_STARTED = "export.started"
    EXPORT_COMPLETED = "export.completed"


@dataclass
class DomainEvent:
    """Base class for all domain events."""
    event_id: str
    event_type: EventType
    aggregate_id: str
    aggregate_type: str
    payload: Dict[str, Any]
    metadata: Dict[str, Any]
    occurred_at: datetime
    user_id: Optional[int] = None
    correlation_id: Optional[str] = None
    
    @classmethod
    def create(
        cls,
        event_type: EventType,
        aggregate_id: str,
        aggregate_type: str,
        payload: Dict[str, Any],
        user_id: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ) -> 'DomainEvent':
        """Factory method to create a domain event."""
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            payload=payload,
            metadata=metadata or {},
            occurred_at=datetime.utcnow(),
            user_id=user_id,
            correlation_id=correlation_id
        )


# Event handler type
EventHandler = Callable[[DomainEvent], Awaitable[None]]


class IEventBus(ABC):
    """Interface for publishing and subscribing to domain events."""
    
    @abstractmethod
    async def publish(self, event: DomainEvent) -> None:
        """Publish a domain event to all registered handlers."""
        pass
    
    @abstractmethod
    async def publish_batch(self, events: List[DomainEvent]) -> None:
        """Publish multiple events in order."""
        pass
    
    @abstractmethod
    def subscribe(
        self,
        event_type: EventType,
        handler: EventHandler
    ) -> None:
        """Subscribe a handler to a specific event type."""
        pass
    
    @abstractmethod
    def unsubscribe(
        self,
        event_type: EventType,
        handler: EventHandler
    ) -> None:
        """Unsubscribe a handler from an event type."""
        pass
    
    @abstractmethod
    def get_handlers(self, event_type: EventType) -> List[EventHandler]:
        """Get all handlers for an event type."""
        pass


class IEventStore(ABC):
    """Interface for persisting and retrieving domain events."""
    
    @abstractmethod
    async def append(self, event: DomainEvent) -> None:
        """Append an event to the event store."""
        pass
    
    @abstractmethod
    async def append_batch(self, events: List[DomainEvent]) -> None:
        """Append multiple events to the store."""
        pass
    
    @abstractmethod
    async def get_events(
        self,
        aggregate_id: str,
        aggregate_type: Optional[str] = None,
        from_version: Optional[int] = None,
        to_version: Optional[int] = None
    ) -> List[DomainEvent]:
        """Get events for a specific aggregate."""
        pass
    
    @abstractmethod
    async def get_events_by_type(
        self,
        event_type: EventType,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[DomainEvent]:
        """Get events of a specific type within a time range."""
        pass
    
    @abstractmethod
    async def get_events_by_correlation_id(
        self,
        correlation_id: str
    ) -> List[DomainEvent]:
        """Get all events with a specific correlation ID."""
        pass
    
    @abstractmethod
    async def get_latest_snapshot(
        self,
        aggregate_id: str,
        aggregate_type: str
    ) -> Optional[Dict[str, Any]]:
        """Get the latest snapshot for an aggregate."""
        pass
    
    @abstractmethod
    async def save_snapshot(
        self,
        aggregate_id: str,
        aggregate_type: str,
        version: int,
        data: Dict[str, Any]
    ) -> None:
        """Save a snapshot of an aggregate state."""
        pass


class IEventHandler(ABC):
    """Base interface for event handlers."""
    
    @abstractmethod
    def handles(self) -> List[EventType]:
        """Return list of event types this handler processes."""
        pass
    
    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """Handle a domain event."""
        pass
    
    @property
    @abstractmethod
    def handler_name(self) -> str:
        """Return the name of this handler for logging."""
        pass


class IEventPublisher(ABC):
    """Interface for components that publish events."""
    
    @abstractmethod
    def set_event_bus(self, event_bus: IEventBus) -> None:
        """Set the event bus for publishing events."""
        pass
    
    @abstractmethod
    async def publish_event(self, event: DomainEvent) -> None:
        """Publish a single event."""
        pass
    
    @abstractmethod
    async def publish_events(self, events: List[DomainEvent]) -> None:
        """Publish multiple events."""
        pass


# Concrete event classes
@dataclass
class DatasetCreatedEvent(DomainEvent):
    """Event raised when a dataset is created."""
    
    @classmethod
    def from_dataset(
        cls,
        dataset_id: int,
        name: str,
        created_by: int,
        tags: List[str],
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'DatasetCreatedEvent':
        """Create event from dataset data."""
        return cls.create(
            event_type=EventType.DATASET_CREATED,
            aggregate_id=str(dataset_id),
            aggregate_type="Dataset",
            payload={
                "name": name,
                "tags": tags,
                "created_by": created_by
            },
            user_id=created_by,
            metadata=metadata
        )


@dataclass
class DatasetUpdatedEvent(DomainEvent):
    """Event raised when a dataset is updated."""
    
    @classmethod
    def from_update(
        cls,
        dataset_id: int,
        changes: Dict[str, Any],
        updated_by: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'DatasetUpdatedEvent':
        """Create event from update data."""
        return cls.create(
            event_type=EventType.DATASET_UPDATED,
            aggregate_id=str(dataset_id),
            aggregate_type="Dataset",
            payload={"changes": changes},
            user_id=updated_by,
            metadata=metadata
        )


@dataclass
class DatasetDeletedEvent(DomainEvent):
    """Event raised when a dataset is deleted."""
    
    @classmethod
    def from_deletion(
        cls,
        dataset_id: int,
        deleted_by: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'DatasetDeletedEvent':
        """Create event from deletion data."""
        return cls.create(
            event_type=EventType.DATASET_DELETED,
            aggregate_id=str(dataset_id),
            aggregate_type="Dataset",
            payload={"deleted_by": deleted_by},
            user_id=deleted_by,
            metadata=metadata
        )


@dataclass
class CommitCreatedEvent(DomainEvent):
    """Event raised when a commit is created."""
    
    @classmethod
    def from_commit(
        cls,
        commit_id: str,
        dataset_id: int,
        message: str,
        author_id: int,
        parent_commit_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'CommitCreatedEvent':
        """Create event from commit data."""
        return cls.create(
            event_type=EventType.COMMIT_CREATED,
            aggregate_id=commit_id,
            aggregate_type="Commit",
            payload={
                "dataset_id": dataset_id,
                "message": message,
                "author_id": author_id,
                "parent_commit_id": parent_commit_id
            },
            user_id=author_id,
            metadata=metadata
        )


@dataclass
class UserDeletedEvent(DomainEvent):
    """Event raised when a user is deleted."""
    
    @classmethod
    def from_deletion(
        cls,
        user_id: int,
        deleted_by: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'UserDeletedEvent':
        """Create event from user deletion."""
        return cls.create(
            event_type=EventType.USER_DELETED,
            aggregate_id=str(user_id),
            aggregate_type="User",
            payload={"deleted_by": deleted_by},
            user_id=deleted_by,
            metadata=metadata
        )


@dataclass
class JobLifecycleEvent(DomainEvent):
    """Base event for job lifecycle changes."""
    
    @classmethod
    def from_job(
        cls,
        job_id: str,
        job_type: str,
        event_type: EventType,
        user_id: int,
        dataset_id: Optional[int] = None,
        additional_data: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'JobLifecycleEvent':
        """Create event from job data."""
        payload = {
            "job_type": job_type,
            "dataset_id": dataset_id
        }
        if additional_data:
            payload.update(additional_data)
            
        return cls.create(
            event_type=event_type,
            aggregate_id=job_id,
            aggregate_type="Job",
            payload=payload,
            user_id=user_id,
            metadata=metadata
        )


@dataclass
class UserCreatedEvent(DomainEvent):
    """Event raised when a user is created."""
    
    @classmethod
    def from_user(
        cls,
        user_id: int,
        soeid: str,
        role_name: str,
        created_by: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'UserCreatedEvent':
        """Create event from user data."""
        return cls.create(
            event_type=EventType.USER_CREATED,
            aggregate_id=str(user_id),
            aggregate_type="User",
            payload={
                "soeid": soeid,
                "role_name": role_name,
                "created_by": created_by
            },
            user_id=created_by,
            metadata=metadata
        )


@dataclass
class UserUpdatedEvent(DomainEvent):
    """Event raised when a user is updated."""
    
    @classmethod
    def from_update(
        cls,
        user_id: int,
        changes: Dict[str, Any],
        updated_by: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'UserUpdatedEvent':
        """Create event from user update."""
        return cls.create(
            event_type=EventType.USER_UPDATED,
            aggregate_id=str(user_id),
            aggregate_type="User",
            payload={"changes": changes},
            user_id=updated_by,
            metadata=metadata
        )


@dataclass
class JobCreatedEvent(DomainEvent):
    """Event raised when a job is created."""
    
    @classmethod
    def from_job_data(
        cls,
        job_id: str,
        job_type: str,
        dataset_id: int,
        user_id: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'JobCreatedEvent':
        """Create event from job creation."""
        return cls.create(
            event_type=EventType.JOB_CREATED,
            aggregate_id=job_id,
            aggregate_type="Job",
            payload={
                "job_type": job_type,
                "dataset_id": dataset_id
            },
            user_id=user_id,
            metadata=metadata
        )


@dataclass
class JobStartedEvent(DomainEvent):
    """Event raised when a job starts executing."""
    
    def __init__(self, job_id: str, job_type: str, dataset_id: int, user_id: int):
        super().__init__(
            event_id=str(uuid.uuid4()),
            event_type=EventType.JOB_STARTED,
            aggregate_id=job_id,
            aggregate_type="Job",
            payload={
                "job_type": job_type,
                "dataset_id": dataset_id
            },
            metadata={},
            occurred_at=datetime.utcnow(),
            user_id=user_id
        )


@dataclass
class JobCompletedEvent(DomainEvent):
    """Event raised when a job completes successfully."""
    
    def __init__(self, job_id: str, job_type: str, dataset_id: int, user_id: int, result: Dict[str, Any]):
        super().__init__(
            event_id=str(uuid.uuid4()),
            event_type=EventType.JOB_COMPLETED,
            aggregate_id=job_id,
            aggregate_type="Job",
            payload={
                "job_type": job_type,
                "dataset_id": dataset_id,
                "result": result
            },
            metadata={},
            occurred_at=datetime.utcnow(),
            user_id=user_id
        )


@dataclass
class JobFailedEvent(DomainEvent):
    """Event raised when a job fails."""
    
    def __init__(self, job_id: str, job_type: str, dataset_id: int, user_id: int, error_message: str):
        super().__init__(
            event_id=str(uuid.uuid4()),
            event_type=EventType.JOB_FAILED,
            aggregate_id=job_id,
            aggregate_type="Job",
            payload={
                "job_type": job_type,
                "dataset_id": dataset_id,
                "error_message": error_message
            },
            metadata={},
            occurred_at=datetime.utcnow(),
            user_id=user_id
        )


@dataclass
class JobCancelledEvent(DomainEvent):
    """Event raised when a job is cancelled."""
    
    @classmethod
    def from_cancellation(
        cls,
        job_id: str,
        cancelled_by: int,
        reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'JobCancelledEvent':
        """Create event from job cancellation."""
        return cls.create(
            event_type=EventType.JOB_CANCELLED,
            aggregate_id=job_id,
            aggregate_type="Job",
            payload={
                "cancelled_by": cancelled_by,
                "reason": reason
            },
            user_id=cancelled_by,
            metadata=metadata
        )


@dataclass  
class PermissionGrantedEvent(DomainEvent):
    """Event raised when permission is granted."""
    
    @classmethod
    def from_grant(
        cls,
        dataset_id: int,
        user_id: int,
        permission_level: str,
        granted_by: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'PermissionGrantedEvent':
        """Create event from permission grant."""
        return cls.create(
            event_type=EventType.DATASET_UPDATED,  # Using existing event type
            aggregate_id=str(dataset_id),
            aggregate_type="Dataset",
            payload={
                "permission_granted": {
                    "user_id": user_id,
                    "permission_level": permission_level,
                    "granted_by": granted_by
                }
            },
            user_id=granted_by,
            metadata=metadata
        )


@dataclass
class BranchDeletedEvent(DomainEvent):
    """Event raised when a branch is deleted."""
    
    @classmethod
    def from_deletion(
        cls,
        dataset_id: int,
        branch_name: str,
        deleted_by: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'BranchDeletedEvent':
        """Create event from branch deletion."""
        return cls.create(
            event_type=EventType.DATASET_UPDATED,  # Using existing event type
            aggregate_id=str(dataset_id),
            aggregate_type="Dataset",
            payload={
                "branch_deleted": branch_name,
                "deleted_by": deleted_by
            },
            user_id=deleted_by,
            metadata=metadata
        )