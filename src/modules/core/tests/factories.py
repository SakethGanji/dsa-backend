from datetime import datetime
from typing import Any, Dict, Optional, Type, TypeVar

from faker import Faker

from ..domain.base import Entity
from ..domain.types import ContentHash, FilePath, UserId

fake = Faker()

T = TypeVar('T')


class Factory:
    """Base factory for creating test data."""
    
    @classmethod
    def create(cls, **kwargs) -> Any:
        """Create an instance with given attributes."""
        raise NotImplementedError
    
    @classmethod
    def create_batch(cls, count: int, **kwargs) -> list:
        """Create multiple instances."""
        return [cls.create(**kwargs) for _ in range(count)]


class UserIdFactory(Factory):
    """Factory for UserId."""
    
    @classmethod
    def create(cls, value: Optional[int] = None) -> UserId:
        return UserId(value or fake.random_int(min=1, max=1000))


class ContentHashFactory(Factory):
    """Factory for ContentHash."""
    
    @classmethod
    def create(cls, value: Optional[str] = None) -> ContentHash:
        if value is None:
            # Generate a valid SHA256 hash (64 hex characters)
            value = fake.sha256()
        return ContentHash(value=value)


class FilePathFactory(Factory):
    """Factory for FilePath."""
    
    @classmethod
    def create(cls, value: Optional[str] = None) -> FilePath:
        if value is None:
            value = f"/data/{fake.uuid4()}/{fake.file_name()}"
        return FilePath(value=value)


class EntityFactory(Factory):
    """Base factory for entities."""
    
    entity_class: Type[Entity] = Entity
    
    @classmethod
    def get_defaults(cls) -> Dict[str, Any]:
        """Get default values for the entity."""
        return {
            'id': fake.random_int(min=1, max=10000),
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
        }
    
    @classmethod
    def create(cls, **kwargs) -> Entity:
        """Create an entity instance."""
        defaults = cls.get_defaults()
        defaults.update(kwargs)
        return cls.entity_class(**defaults)


def build_test_data(factory_class: Type[Factory], count: int = 1, **kwargs):
    """Helper function to build test data."""
    if count == 1:
        return factory_class.create(**kwargs)
    return factory_class.create_batch(count, **kwargs)