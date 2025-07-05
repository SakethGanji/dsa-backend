"""Database connection pool and Unit of Work implementation."""

import asyncio
from contextlib import asynccontextmanager
from typing import Optional, AsyncContextManager
import asyncpg
from asyncpg import Pool, Connection
from .abstractions import IUnitOfWork
from .infrastructure.postgres import PostgresUnitOfWork


class DatabasePool:
    """Manages the PostgreSQL connection pool."""
    
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._pool: Optional[Pool] = None
    
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
    
    async def _init_connection(self, conn):
        """Initialize each connection with the proper search_path."""
        await conn.execute("SET search_path TO dsa_core, dsa_jobs, dsa_auth, public")
    
    async def close(self):
        """Close all connections in the pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
    
    @asynccontextmanager
    async def acquire(self) -> AsyncContextManager[Connection]:
        """Acquire a connection from the pool."""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        
        async with self._pool.acquire() as connection:
            yield connection



class UnitOfWorkFactory:
    """Factory for creating Unit of Work instances."""
    
    def __init__(self, pool: DatabasePool):
        self._pool = pool
    
    def create(self) -> IUnitOfWork:
        """Create a new Unit of Work instance."""
        return PostgresUnitOfWork(self._pool)