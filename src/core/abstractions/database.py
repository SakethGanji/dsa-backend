"""Generic database abstraction interfaces."""

from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List, AsyncContextManager
from datetime import datetime


class IDatabaseConnection(ABC):
    """Generic database connection interface."""
    
    @abstractmethod
    async def execute(self, query: str, *args) -> str:
        """Execute a query without returning results."""
        pass
    
    @abstractmethod
    async def executemany(self, query: str, args: List[tuple]) -> None:
        """Execute a query multiple times with different arguments."""
        pass
    
    @abstractmethod
    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Execute a query and return a single row as a dictionary."""
        pass
    
    @abstractmethod
    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a query and return all rows as list of dictionaries."""
        pass
    
    @abstractmethod
    async def fetchval(self, query: str, *args, column: int = 0) -> Any:
        """Execute a query and return a single value."""
        pass
    
    @abstractmethod
    async def transaction(self) -> AsyncContextManager['ITransaction']:
        """Start a database transaction."""
        pass


class ITransaction(ABC):
    """Generic database transaction interface."""
    
    @abstractmethod
    async def commit(self) -> None:
        """Commit the transaction."""
        pass
    
    @abstractmethod
    async def rollback(self) -> None:
        """Rollback the transaction."""
        pass


class IDatabasePool(ABC):
    """Generic database connection pool interface."""
    
    @abstractmethod
    async def acquire(self) -> AsyncContextManager[IDatabaseConnection]:
        """
        Acquire a connection from the pool.
        
        Returns:
            AsyncContextManager that yields a IDatabaseConnection
        """
        pass
    
    @abstractmethod
    async def release(self, connection: IDatabaseConnection) -> None:
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
        """Execute a query without returning results using a pooled connection."""
        pass
    
    @abstractmethod
    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Execute a query and return a single row using a pooled connection."""
        pass
    
    @abstractmethod
    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a query and return all rows using a pooled connection."""
        pass