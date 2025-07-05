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
        
        Implementation Notes:
        In new Git-like system:
        1. Validate backend implements required methods
        2. Register in _backends dict
        3. Log registration for debugging
        4. Consider adding S3/cloud backends later
        
        Current backends:
        - local: LocalStorageBackend for file operations
        - Future: S3Backend, AzureBackend, etc.
        
        Args:
            name: Name identifier for the backend
            backend_class: The backend class to register
        """
        raise NotImplementedError("Implement backend registration")
    
    @classmethod
    def create(cls, backend_type: str = "local", **kwargs) -> StorageBackend:
        """Create a storage backend instance.
        
        Implementation Notes:
        1. Validate backend_type exists
        2. Get backend class from registry
        3. Pass configuration kwargs
        4. Initialize backend with Git-like system support
        
        Configuration per backend:
        - local: base_path for file storage
        - future S3: bucket, prefix, credentials
        
        Example:
        backend = StorageFactory.create(
            "local",
            base_path="/data",
            enable_compression=True
        )
        
        Args:
            backend_type: Type of backend to create
            **kwargs: Arguments to pass to the backend constructor
            
        Returns:
            StorageBackend instance
        """
        raise NotImplementedError("Implement backend creation")
    
    @classmethod
    def get_instance(cls, backend_type: str = "local", **kwargs) -> StorageBackend:
        """Get a singleton instance of the storage backend.
        
        Implementation Notes:
        1. Check if instance exists and matches type
        2. Create new instance if needed
        3. Handle default configuration
        4. Store singleton reference
        
        Default configurations:
        - local: /data directory relative to project root
        - Samples: /data/samples/
        - Temporary: /data/temp/
        - Export cache: /data/export/
        
        Singleton behavior:
        - Reuses instance if backend_type matches
        - Creates new if type changes
        - Thread-safe initialization needed
        
        Args:
            backend_type: Type of backend to create
            **kwargs: Arguments to pass to the backend constructor
            
        Returns:
            StorageBackend instance
        """
        raise NotImplementedError("Implement singleton pattern")
    
    @classmethod
    def reset(cls) -> None:
        """Reset the singleton instance.
        
        Implementation Notes:
        1. Clear singleton instance
        2. Reset backend type
        3. Clean up any resources
        4. Used mainly for testing
        
        Usage:
        - Test teardown
        - Configuration changes
        - Backend switching
        """
        raise NotImplementedError("Implement singleton reset")