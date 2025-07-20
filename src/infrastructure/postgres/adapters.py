"""Adapters to convert between asyncpg types and generic interfaces."""

from typing import Any, Optional, Dict, List, AsyncContextManager
from contextlib import asynccontextmanager
import asyncpg
from ...core.abstractions.database import IDatabaseConnection, ITransaction, IDatabasePool


class AsyncpgConnectionAdapter(IDatabaseConnection):
    """Adapter to wrap asyncpg.Connection with generic interface."""
    
    def __init__(self, connection: asyncpg.Connection):
        self._conn = connection
    
    async def execute(self, query: str, *args) -> str:
        """Execute a query without returning results."""
        return await self._conn.execute(query, *args)
    
    async def executemany(self, query: str, args: List[tuple]) -> None:
        """Execute a query multiple times with different arguments."""
        await self._conn.executemany(query, args)
    
    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Execute a query and return a single row as a dictionary."""
        row = await self._conn.fetchrow(query, *args)
        return dict(row) if row else None
    
    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a query and return all rows as list of dictionaries."""
        rows = await self._conn.fetch(query, *args)
        return [dict(row) for row in rows]
    
    async def fetchval(self, query: str, *args, column: int = 0) -> Any:
        """Execute a query and return a single value."""
        return await self._conn.fetchval(query, *args, column=column)
    
    @asynccontextmanager
    async def transaction(self):
        """Start a database transaction."""
        async with self._conn.transaction():
            yield AsyncpgTransactionAdapter()
    
    @property
    def raw_connection(self) -> asyncpg.Connection:
        """Get the underlying asyncpg connection for legacy code."""
        return self._conn


class AsyncpgTransactionAdapter(ITransaction):
    """Adapter for asyncpg transactions."""
    
    async def commit(self) -> None:
        """Commit is handled automatically by asyncpg context manager."""
        pass
    
    async def rollback(self) -> None:
        """Rollback is handled automatically by asyncpg context manager."""
        pass


class AsyncpgPoolAdapter(IDatabasePool):
    """Adapter to wrap asyncpg.Pool with generic interface."""
    
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
    
    @asynccontextmanager
    async def acquire(self) -> AsyncContextManager[IDatabaseConnection]:
        """Acquire a connection from the pool."""
        async with self._pool.acquire() as conn:
            yield AsyncpgConnectionAdapter(conn)
    
    async def release(self, connection: IDatabaseConnection) -> None:
        """Release is handled automatically by asyncpg context manager."""
        pass
    
    async def close(self) -> None:
        """Close the connection pool."""
        await self._pool.close()
    
    async def execute(self, query: str, *args) -> str:
        """Execute a query without returning results using a pooled connection."""
        return await self._pool.execute(query, *args)
    
    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Execute a query and return a single row using a pooled connection."""
        row = await self._pool.fetchrow(query, *args)
        return dict(row) if row else None
    
    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a query and return all rows using a pooled connection."""
        rows = await self._pool.fetch(query, *args)
        return [dict(row) for row in rows]
    
    @property
    def raw_pool(self) -> asyncpg.Pool:
        """Get the underlying asyncpg pool for legacy code."""
        return self._pool


def convert_record_to_dict(record: asyncpg.Record) -> Dict[str, Any]:
    """Convert an asyncpg.Record to a dictionary."""
    return dict(record) if record else None


def convert_records_to_dicts(records: List[asyncpg.Record]) -> List[Dict[str, Any]]:
    """Convert a list of asyncpg.Records to list of dictionaries."""
    return [dict(record) for record in records]