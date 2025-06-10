"""Storage backend factory for dependency injection."""
from typing import Dict, Type
from .backend import StorageBackend
from .local_backend import LocalStorageBackend


class StorageFactory:
    """Factory for creating storage backend instances."""
    
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
    
    @classmethod
    def create(cls, backend_type: str = "local", **kwargs) -> StorageBackend:
        """Create a storage backend instance.
        
        Args:
            backend_type: Type of backend to create
            **kwargs: Arguments to pass to the backend constructor
            
        Returns:
            StorageBackend instance
        """
        if backend_type not in cls._backends:
            raise ValueError(f"Unknown backend type: {backend_type}")
        
        backend_class = cls._backends[backend_type]
        return backend_class(**kwargs)
    
    @classmethod
    def get_instance(cls, backend_type: str = "local", **kwargs) -> StorageBackend:
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
            
            cls._instance = cls.create(backend_type, **kwargs)
            cls._backend_type = backend_type
        
        return cls._instance
    
    @classmethod
    def reset(cls) -> None:
        """Reset the singleton instance."""
        cls._instance = None
        cls._backend_type = None