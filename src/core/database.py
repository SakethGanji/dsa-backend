"""Database connection pool and Unit of Work implementation."""

import asyncio
from contextlib import asynccontextmanager
from typing import Optional, AsyncContextManager
import asyncpg
from asyncpg import Pool, Connection
from abc import ABC, abstractmethod


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


class IUnitOfWork(ABC):
    """Abstract Unit of Work interface."""
    
    @abstractmethod
    async def begin(self):
        """Begin a new transaction."""
        pass
    
    @abstractmethod
    async def commit(self):
        """Commit the current transaction."""
        pass
    
    @abstractmethod
    async def rollback(self):
        """Rollback the current transaction."""
        pass
    
    @property
    @abstractmethod
    def connection(self) -> Connection:
        """Get the current database connection."""
        pass


class PostgresUnitOfWork(IUnitOfWork):
    """PostgreSQL implementation of Unit of Work pattern."""
    
    def __init__(self, pool: DatabasePool):
        self._pool = pool
        self._connection: Optional[Connection] = None
        self._transaction = None
    
    async def __aenter__(self):
        """Enter the context manager."""
        await self.begin()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager."""
        if exc_type:
            await self.rollback()
        else:
            await self.commit()
        
        if self._connection:
            await self._pool._pool.release(self._connection)
            self._connection = None
    
    async def begin(self):
        """Begin a new transaction."""
        if self._connection is None:
            self._connection = await self._pool._pool.acquire()
            self._transaction = self._connection.transaction()
            await self._transaction.start()
    
    async def commit(self):
        """Commit the current transaction."""
        if self._transaction:
            await self._transaction.commit()
            self._transaction = None
    
    async def rollback(self):
        """Rollback the current transaction."""
        if self._transaction:
            await self._transaction.rollback()
            self._transaction = None
    
    @property
    def connection(self) -> Connection:
        """Get the current database connection."""
        if not self._connection:
            raise RuntimeError("No active connection in unit of work")
        return self._connection


class UnitOfWorkFactory:
    """Factory for creating Unit of Work instances."""
    
    def __init__(self, pool: DatabasePool):
        self._pool = pool
    
    def create(self) -> IUnitOfWork:
        """Create a new Unit of Work instance."""
        return PostgresUnitOfWork(self._pool)