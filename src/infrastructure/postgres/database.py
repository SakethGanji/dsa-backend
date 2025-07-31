"""Database connection pool and Unit of Work implementation."""

import asyncio
from contextlib import asynccontextmanager
from typing import Optional, AsyncContextManager, Dict, Any, List
import asyncpg
from asyncpg import Pool, Connection
# Remove interface imports
from .uow import PostgresUnitOfWork
from .adapters import AsyncpgPoolAdapter, AsyncpgConnectionAdapter


class DatabasePool:
    """Manages the PostgreSQL connection pool."""
    
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._pool: Optional[Pool] = None
        self._adapter: Optional[AsyncpgPoolAdapter] = None
    
    async def initialize(self, min_size: int = 10, max_size: int = 20):
        """Initialize the connection pool."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=min_size,
                max_size=max_size,
                command_timeout=60,
                init=self._init_connection
            )
            self._adapter = AsyncpgPoolAdapter(self._pool)
    
    async def _init_connection(self, conn):
        """Initialize each connection with the proper search_path."""
        await conn.execute("SET search_path TO dsa_core, dsa_jobs, dsa_auth, dsa_search, dsa_events, dsa_audit, dsa_staging")
    
    async def close(self):
        """Close all connections in the pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
    
    @asynccontextmanager
    async def acquire(self) -> AsyncContextManager:
        """Acquire a connection from the pool."""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        
        async with self._pool.acquire() as connection:
            yield AsyncpgConnectionAdapter(connection)
    
    async def release(self, connection) -> None:
        """Release a connection back to the pool."""
        # Connection release is handled by context manager
        pass
    
    async def execute(self, query: str, *args) -> str:
        """Execute a query without returning results."""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        return await self._pool.execute(query, *args)
    
    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Execute a query and return a single row."""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        row = await self._pool.fetchrow(query, *args)
        return dict(row) if row else None
    
    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a query and return all rows."""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        rows = await self._pool.fetch(query, *args)
        return [dict(row) for row in rows]



class UnitOfWorkFactory:
    """Factory for creating Unit of Work instances."""
    
    def __init__(self, pool: DatabasePool):
        self._pool = pool
    
    def create(self):
        """Create a new Unit of Work instance."""
        return PostgresUnitOfWork(self._pool)