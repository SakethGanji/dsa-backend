import pytest
from datetime import datetime
from uuid import UUID

from ..domain.base import (
    Entity, AggregateRoot, ValueObject, DomainEvent,
    Specification, AndSpecification, OrSpecification, NotSpecification
)


class TestEntity:
    """Test Entity base class."""
    
    def test_entity_creation(self):
        """Test creating an entity."""
        entity = Entity(id=1)
        assert entity.id == 1
        assert isinstance(entity.created_at, datetime)
        assert isinstance(entity.updated_at, datetime)
    
    def test_entity_equality(self):
        """Test entity equality based on ID."""
        entity1 = Entity(id=1)
        entity2 = Entity(id=1)
        entity3 = Entity(id=2)
        entity4 = Entity(id=None)
        
        assert entity1 == entity2
        assert entity1 != entity3
        assert entity1 != entity4
        assert entity4 != entity4  # Entities without ID are never equal
    
    def test_entity_hash(self):
        """Test entity hashing."""
        entity1 = Entity(id=1)
        entity2 = Entity(id=1)
        entity3 = Entity(id=2)
        
        assert hash(entity1) == hash(entity2)
        assert hash(entity1) != hash(entity3)


class TestDomainEvent:
    """Test DomainEvent class."""
    
    def test_event_creation(self):
        """Test creating a domain event."""
        event = DomainEvent()
        
        assert isinstance(event.event_id, UUID)
        assert isinstance(event.occurred_at, datetime)
        assert event.aggregate_id is None
        assert event.event_type == "DomainEvent"
    
    def test_custom_event(self):
        """Test custom event class."""
        class UserCreatedEvent(DomainEvent):
            user_id: int
            
            def __init__(self, user_id: int):
                super().__init__()
                self.user_id = user_id
        
        event = UserCreatedEvent(user_id=123)
        assert event.event_type == "UserCreatedEvent"
        assert event.user_id == 123


class TestAggregateRoot:
    """Test AggregateRoot class."""
    
    def test_aggregate_creation(self):
        """Test creating an aggregate root."""
        aggregate = AggregateRoot(id=1)
        assert aggregate.id == 1
        assert aggregate.events == []
    
    def test_add_event(self):
        """Test adding events to aggregate."""
        aggregate = AggregateRoot(id=1)
        event1 = DomainEvent()
        event2 = DomainEvent()
        
        aggregate.add_event(event1)
        aggregate.add_event(event2)
        
        assert len(aggregate.events) == 2
        assert event1.aggregate_id == 1
        assert event2.aggregate_id == 1
    
    def test_clear_events(self):
        """Test clearing events."""
        aggregate = AggregateRoot(id=1)
        aggregate.add_event(DomainEvent())
        aggregate.add_event(DomainEvent())
        
        assert len(aggregate.events) == 2
        
        aggregate.clear_events()
        assert len(aggregate.events) == 0


class TestValueObject:
    """Test ValueObject base class."""
    
    def test_value_object_equality(self):
        """Test value object equality based on attributes."""
        class Money(ValueObject):
            def __init__(self, amount: float, currency: str):
                self.amount = amount
                self.currency = currency
        
        money1 = Money(100, "USD")
        money2 = Money(100, "USD")
        money3 = Money(100, "EUR")
        money4 = Money(200, "USD")
        
        assert money1 == money2
        assert money1 != money3
        assert money1 != money4
    
    def test_value_object_hash(self):
        """Test value object hashing."""
        class Point(ValueObject):
            def __init__(self, x: int, y: int):
                self.x = x
                self.y = y
        
        point1 = Point(1, 2)
        point2 = Point(1, 2)
        point3 = Point(2, 1)
        
        assert hash(point1) == hash(point2)
        assert hash(point1) != hash(point3)


class TestSpecification:
    """Test Specification pattern."""
    
    class EvenNumberSpec(Specification[int]):
        def is_satisfied_by(self, number: int) -> bool:
            return number % 2 == 0
    
    class GreaterThanSpec(Specification[int]):
        def __init__(self, value: int):
            self.value = value
        
        def is_satisfied_by(self, number: int) -> bool:
            return number > self.value
    
    def test_basic_specification(self):
        """Test basic specification."""
        even_spec = self.EvenNumberSpec()
        
        assert even_spec.is_satisfied_by(2)
        assert even_spec.is_satisfied_by(4)
        assert not even_spec.is_satisfied_by(3)
        assert not even_spec.is_satisfied_by(5)
    
    def test_and_specification(self):
        """Test AND specification."""
        even_spec = self.EvenNumberSpec()
        greater_than_10 = self.GreaterThanSpec(10)
        
        combined = even_spec.and_(greater_than_10)
        
        assert combined.is_satisfied_by(12)
        assert combined.is_satisfied_by(20)
        assert not combined.is_satisfied_by(8)  # Even but not > 10
        assert not combined.is_satisfied_by(11)  # > 10 but not even
    
    def test_or_specification(self):
        """Test OR specification."""
        even_spec = self.EvenNumberSpec()
        greater_than_10 = self.GreaterThanSpec(10)
        
        combined = even_spec.or_(greater_than_10)
        
        assert combined.is_satisfied_by(2)   # Even
        assert combined.is_satisfied_by(15)  # > 10
        assert combined.is_satisfied_by(12)  # Both
        assert not combined.is_satisfied_by(7)  # Neither
    
    def test_not_specification(self):
        """Test NOT specification."""
        even_spec = self.EvenNumberSpec()
        odd_spec = even_spec.not_()
        
        assert odd_spec.is_satisfied_by(1)
        assert odd_spec.is_satisfied_by(3)
        assert not odd_spec.is_satisfied_by(2)
        assert not odd_spec.is_satisfied_by(4)