from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar
from uuid import UUID, uuid4


@dataclass
class DomainEvent:
    """Base class for domain events."""
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=datetime.utcnow)
    aggregate_id: Optional[int] = None
    event_type: str = field(init=False)
    
    def __post_init__(self):
        self.event_type = self.__class__.__name__


@dataclass
class Entity:
    """Base class for all entities."""
    id: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def __eq__(self, other):
        if not isinstance(other, Entity):
            return False
        return self.id == other.id and self.id is not None
    
    def __hash__(self):
        return hash(self.id)


@dataclass
class AggregateRoot(Entity):
    """Base class for aggregate roots with event sourcing support."""
    _events: List[DomainEvent] = field(default_factory=list, init=False, repr=False)
    
    def add_event(self, event: DomainEvent):
        """Add a domain event."""
        event.aggregate_id = self.id
        self._events.append(event)
    
    def clear_events(self):
        """Clear all events."""
        self._events.clear()
    
    @property
    def events(self) -> List[DomainEvent]:
        """Get all uncommitted events."""
        return list(self._events)


@dataclass
class ValueObject(ABC):
    """Base class for value objects."""
    
    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.__dict__ == other.__dict__
    
    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))


T = TypeVar('T', bound=Entity)
K = TypeVar('K')


class Repository(ABC, Generic[T, K]):
    """Base repository interface."""
    
    @abstractmethod
    async def find_by_id(self, id: K) -> Optional[T]:
        """Find entity by ID."""
        pass
    
    @abstractmethod
    async def save(self, entity: T) -> T:
        """Save entity."""
        pass
    
    @abstractmethod
    async def delete(self, entity: T) -> None:
        """Delete entity."""
        pass
    
    @abstractmethod
    async def find_all(self) -> List[T]:
        """Find all entities."""
        pass


class Specification(ABC, Generic[T]):
    """Base specification pattern for queries."""
    
    @abstractmethod
    def is_satisfied_by(self, entity: T) -> bool:
        """Check if entity satisfies the specification."""
        pass
    
    def and_(self, other: 'Specification[T]') -> 'AndSpecification[T]':
        """Combine with another specification using AND."""
        return AndSpecification(self, other)
    
    def or_(self, other: 'Specification[T]') -> 'OrSpecification[T]':
        """Combine with another specification using OR."""
        return OrSpecification(self, other)
    
    def not_(self) -> 'NotSpecification[T]':
        """Negate the specification."""
        return NotSpecification(self)


class AndSpecification(Specification[T]):
    """AND specification combinator."""
    
    def __init__(self, left: Specification[T], right: Specification[T]):
        self.left = left
        self.right = right
    
    def is_satisfied_by(self, entity: T) -> bool:
        return self.left.is_satisfied_by(entity) and self.right.is_satisfied_by(entity)


class OrSpecification(Specification[T]):
    """OR specification combinator."""
    
    def __init__(self, left: Specification[T], right: Specification[T]):
        self.left = left
        self.right = right
    
    def is_satisfied_by(self, entity: T) -> bool:
        return self.left.is_satisfied_by(entity) or self.right.is_satisfied_by(entity)


class NotSpecification(Specification[T]):
    """NOT specification combinator."""
    
    def __init__(self, spec: Specification[T]):
        self.spec = spec
    
    def is_satisfied_by(self, entity: T) -> bool:
        return not self.spec.is_satisfied_by(entity)