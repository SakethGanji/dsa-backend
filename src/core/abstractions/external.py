"""Abstract interfaces for external dependencies."""

from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, AsyncContextManager
import asyncpg
from datetime import datetime


class IConnectionPool(ABC):
    """Interface for database connection pool."""
    
    @abstractmethod
    async def acquire(self) -> AsyncContextManager[asyncpg.Connection]:
        """
        Acquire a connection from the pool.
        
        Returns:
            AsyncContextManager that yields a Connection
        """
        pass
    
    @abstractmethod
    async def release(self, connection: asyncpg.Connection) -> None:
        """
        Release a connection back to the pool.
        
        Args:
            connection: The connection to release
        """
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close the connection pool."""
        pass
    
    @abstractmethod
    async def execute(self, query: str, *args) -> str:
        """Execute a query without returning results."""
        pass
    
    @abstractmethod
    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """Execute a query and return a single row."""
        pass
    
    @abstractmethod
    async def fetch(self, query: str, *args) -> list[asyncpg.Record]:
        """Execute a query and return all rows."""
        pass


class IFileStorage(ABC):
    """Interface for file storage operations."""
    
    @abstractmethod
    async def store(self, file_path: str, content: bytes, metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Store a file and return its identifier.
        
        Args:
            file_path: Path/name for the file
            content: File content as bytes
            metadata: Optional metadata to store with the file
            
        Returns:
            Unique identifier for the stored file
        """
        pass
    
    @abstractmethod
    async def retrieve(self, file_id: str) -> bytes:
        """
        Retrieve a file by its identifier.
        
        Args:
            file_id: Unique identifier of the file
            
        Returns:
            File content as bytes
            
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        pass
    
    @abstractmethod
    async def delete(self, file_id: str) -> None:
        """
        Delete a file by its identifier.
        
        Args:
            file_id: Unique identifier of the file
            
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        pass
    
    @abstractmethod
    async def exists(self, file_id: str) -> bool:
        """
        Check if a file exists.
        
        Args:
            file_id: Unique identifier of the file
            
        Returns:
            True if file exists, False otherwise
        """
        pass
    
    @abstractmethod
    async def get_metadata(self, file_id: str) -> Dict[str, Any]:
        """
        Get metadata for a file.
        
        Args:
            file_id: Unique identifier of the file
            
        Returns:
            File metadata
            
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        pass


class IAuthenticationService(ABC):
    """Interface for authentication operations."""
    
    @abstractmethod
    async def verify_token(self, token: str) -> Dict[str, Any]:
        """
        Verify an authentication token and return token data.
        
        Args:
            token: Authentication token to verify
            
        Returns:
            Dict containing token claims/data
            
        Raises:
            AuthenticationError: If token is invalid or expired
        """
        pass
    
    @abstractmethod
    async def create_token(self, user_id: int, additional_claims: Optional[Dict[str, Any]] = None) -> str:
        """
        Create an authentication token for a user.
        
        Args:
            user_id: ID of the user
            additional_claims: Optional additional claims to include
            
        Returns:
            Authentication token string
        """
        pass
    
    @abstractmethod
    async def refresh_token(self, refresh_token: str) -> str:
        """
        Refresh an authentication token.
        
        Args:
            refresh_token: Refresh token
            
        Returns:
            New authentication token
            
        Raises:
            AuthenticationError: If refresh token is invalid
        """
        pass
    
    @abstractmethod
    async def revoke_token(self, token: str) -> None:
        """
        Revoke an authentication token.
        
        Args:
            token: Token to revoke
        """
        pass


class IConfigurationProvider(ABC):
    """Interface for configuration management."""
    
    @abstractmethod
    def get_setting(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration setting.
        
        Args:
            key: Setting key
            default: Default value if key not found
            
        Returns:
            Setting value or default
        """
        pass
    
    @abstractmethod
    def get_int(self, key: str, default: int = 0) -> int:
        """Get an integer configuration setting."""
        pass
    
    @abstractmethod
    def get_str(self, key: str, default: str = "") -> str:
        """Get a string configuration setting."""
        pass
    
    @abstractmethod
    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get a boolean configuration setting."""
        pass
    
    @abstractmethod
    def get_list(self, key: str, default: list = None) -> list:
        """Get a list configuration setting."""
        pass
    
    @abstractmethod
    def reload(self) -> None:
        """Reload configuration from source."""
        pass


class ICacheService(ABC):
    """Interface for caching operations."""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        pass
    
    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Set a value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (None for no expiry)
        """
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> None:
        """
        Delete a value from cache.
        
        Args:
            key: Cache key
        """
        pass
    
    @abstractmethod
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if key exists, False otherwise
        """
        pass
    
    @abstractmethod
    async def clear(self) -> None:
        """Clear all cached values."""
        pass


class IMessageQueue(ABC):
    """Interface for message queue operations."""
    
    @abstractmethod
    async def publish(self, topic: str, message: Dict[str, Any]) -> None:
        """
        Publish a message to a topic.
        
        Args:
            topic: Topic/queue name
            message: Message data
        """
        pass
    
    @abstractmethod
    async def subscribe(self, topic: str, handler: callable) -> None:
        """
        Subscribe to messages from a topic.
        
        Args:
            topic: Topic/queue name
            handler: Async function to handle messages
        """
        pass
    
    @abstractmethod
    async def unsubscribe(self, topic: str) -> None:
        """
        Unsubscribe from a topic.
        
        Args:
            topic: Topic/queue name
        """
        pass


class IMetricsCollector(ABC):
    """Interface for metrics collection."""
    
    @abstractmethod
    def increment(self, metric: str, value: int = 1, tags: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric."""
        pass
    
    @abstractmethod
    def gauge(self, metric: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric."""
        pass
    
    @abstractmethod
    def histogram(self, metric: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram metric."""
        pass
    
    @abstractmethod
    def timing(self, metric: str, duration: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Record a timing metric in milliseconds."""
        pass