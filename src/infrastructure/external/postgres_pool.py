"""PostgreSQL connection pool implementation."""

from typing import Optional, AsyncContextManager
import asyncpg
from contextlib import asynccontextmanager
from src.core.abstractions.external import IConnectionPool


class PostgresConnectionPool(IConnectionPool):
    """PostgreSQL implementation of IConnectionPool."""
    
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
    
    @asynccontextmanager
    async def acquire(self) -> AsyncContextManager[asyncpg.Connection]:
        """Acquire a connection from the pool."""
        async with self._pool.acquire() as connection:
            yield connection
    
    async def release(self, connection: asyncpg.Connection) -> None:
        """Release a connection back to the pool."""
        await self._pool.release(connection)
    
    async def close(self) -> None:
        """Close the connection pool."""
        await self._pool.close()
    
    async def execute(self, query: str, *args) -> str:
        """Execute a query without returning results."""
        async with self.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """Execute a query and return a single row."""
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args)
    
    async def fetch(self, query: str, *args) -> list[asyncpg.Record]:
        """Execute a query and return all rows."""
        async with self.acquire() as conn:
            return await conn.fetch(query, *args)