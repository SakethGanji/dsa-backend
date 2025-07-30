"""PostgreSQL connection pool implementation."""

from typing import Optional, AsyncContextManager, Dict, Any, List
import asyncpg
from contextlib import asynccontextmanager
# Remove interface imports
from ..postgres.adapters import AsyncpgConnectionAdapter


class PostgresConnectionPool:
    """PostgreSQL implementation of IDatabasePool."""
    
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
    
    @asynccontextmanager
    async def acquire(self) -> AsyncContextManager:
        """Acquire a connection from the pool."""
        async with self._pool.acquire() as connection:
            yield AsyncpgConnectionAdapter(connection)
    
    async def release(self, connection) -> None:
        """Release a connection back to the pool."""
        # Connection release is handled by context manager
        pass
    
    async def close(self) -> None:
        """Close the connection pool."""
        await self._pool.close()
    
    async def execute(self, query: str, *args) -> str:
        """Execute a query without returning results."""
        return await self._pool.execute(query, *args)
    
    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Execute a query and return a single row."""
        row = await self._pool.fetchrow(query, *args)
        return dict(row) if row else None
    
    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a query and return all rows."""
        rows = await self._pool.fetch(query, *args)
        return [dict(row) for row in rows]