"""Storage module interfaces.

This module defines the interfaces for storage operations, ensuring
clean separation between abstract storage operations and concrete implementations.
"""

from abc import ABC, abstractmethod
from typing import BinaryIO, List, Dict, Any, Optional
from pathlib import Path
import io


class IStorageBackend(ABC):
    """Core storage operations for raw byte handling.
    
    This interface defines low-level storage operations that handle raw bytes.
    It is agnostic to file formats and focuses purely on storage mechanics.
    Implementations can be local filesystem, S3, Azure Blob, GCS, etc.
    """
    
    @abstractmethod
    async def write_stream(self, path: str, stream: BinaryIO) -> None:
        """Write content from stream (memory-efficient).
        
        Args:
            path: The storage path where the file should be written.
            stream: A binary stream to read from. Will be read in chunks.
        
        Raises:
            StorageWriteError: If the write operation fails.
        """
        pass
    
    @abstractmethod
    async def read_stream(self, path: str) -> io.BytesIO:
        """Read content as stream.
        
        Args:
            path: The storage path to read from.
        
        Returns:
            A BytesIO stream containing the file content.
        
        Raises:
            StorageReadError: If the read operation fails.
            FileNotFoundError: If the file doesn't exist.
        """
        pass
    
    @abstractmethod
    async def delete_file(self, path: str) -> None:
        """Delete a file.
        
        Args:
            path: The storage path to delete.
        
        Raises:
            StorageException: If the delete operation fails.
            FileNotFoundError: If the file doesn't exist.
        """
        pass
    
    @abstractmethod
    async def exists(self, path: str) -> bool:
        """Check if file exists.
        
        Args:
            path: The storage path to check.
        
        Returns:
            True if the file exists, False otherwise.
        """
        pass
    
    @abstractmethod
    async def list_files(self, prefix: str) -> List[str]:
        """List files with prefix.
        
        Args:
            prefix: The path prefix to search for.
        
        Returns:
            List of file paths that match the prefix.
        """
        pass
    
    @abstractmethod
    async def get_file_info(self, path: str) -> Dict[str, Any]:
        """Get file metadata.
        
        Args:
            path: The storage path to get info for.
        
        Returns:
            Dictionary containing file metadata (size, modified time, etc.)
        
        Raises:
            FileNotFoundError: If the file doesn't exist.
        """
        pass


class IStorageFactory(ABC):
    """Storage backend factory interface.
    
    This interface enables creation of storage backends based on configuration,
    supporting multiple storage types (local, S3, etc.) through a common interface.
    """
    
    @abstractmethod
    def create_backend(self, backend_type: str, **config) -> IStorageBackend:
        """Create storage backend instance.
        
        Args:
            backend_type: The type of backend to create ('local', 's3', etc.)
            **config: Backend-specific configuration parameters.
        
        Returns:
            An instance of IStorageBackend.
        
        Raises:
            ValueError: If the backend_type is not supported.
        """
        pass