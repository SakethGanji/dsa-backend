"""Storage backend factory for dependency injection."""
from typing import Dict, Type
from .backend import StorageBackend
from .local_backend import LocalStorageBackend
from .interfaces import IStorageBackend, IStorageFactory


class StorageFactory(IStorageFactory):
    """Factory for creating storage backend instances implementing IStorageFactory."""
    
    _backends: Dict[str, Type[StorageBackend]] = {
        "local": LocalStorageBackend,
    }
    
    _instance: StorageBackend = None
    _backend_type: str = None
    
    @classmethod
    def register_backend(cls, name: str, backend_class: Type[StorageBackend]) -> None:
        """Register a new storage backend type.
        
        Args:
            name: Name identifier for the backend
            backend_class: The backend class to register
        """
        cls._backends[name] = backend_class
    
    def create_backend(self, backend_type: str = "local", **kwargs) -> IStorageBackend:
        """Create a storage backend instance.
        
        Implements IStorageFactory.create_backend method.
        
        Args:
            backend_type: Type of backend to create
            **kwargs: Arguments to pass to the backend constructor
            
        Returns:
            StorageBackend instance
        """
        if backend_type not in StorageFactory._backends:
            raise ValueError(f"Unknown backend type: {backend_type}")
        
        backend_class = StorageFactory._backends[backend_type]
        instance = backend_class(**kwargs)
        # Ensure the instance implements IStorageBackend
        if not isinstance(instance, IStorageBackend):
            raise TypeError(f"Backend {backend_type} does not implement IStorageBackend")
        return instance
    
    @classmethod
    def get_instance(cls, backend_type: str = "local", **kwargs) -> IStorageBackend:
        """Get a singleton instance of the storage backend.
        
        Args:
            backend_type: Type of backend to create
            **kwargs: Arguments to pass to the backend constructor
            
        Returns:
            StorageBackend instance
        """
        if cls._instance is None or cls._backend_type != backend_type:
            # Default to local data directory if not specified
            if backend_type == "local" and "base_path" not in kwargs:
                import os
                # Use relative path from project root
                base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
                kwargs["base_path"] = base_path
            
            cls._instance = cls.create_backend(backend_type, **kwargs)
            cls._backend_type = backend_type
        
        return cls._instance
    
    @classmethod
    def reset(cls) -> None:
        """Reset the singleton instance."""
        cls._instance = None
        cls._backend_type = None