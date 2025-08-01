"""PostgreSQL Unit of Work implementation."""

from typing import Optional, Union
import asyncpg

from .adapters import AsyncpgConnectionAdapter
from .user_repo import PostgresUserRepository
from .dataset_repo import PostgresDatasetRepository
from .versioning_repo import PostgresCommitRepository
from .job_repo import PostgresJobRepository
from .table_reader import PostgresTableReader
from .search_repository import PostgresSearchRepository
from .exploration_repo import PostgresExplorationRepository


class PostgresUnitOfWork:
    """PostgreSQL implementation of Unit of Work pattern."""
    
    def __init__(self, pool):
        self._pool = pool
        self._connection: Optional[asyncpg.Connection] = None
        self._is_adapted = False
        self._transaction = None
        self._users = None
        self._datasets = None
        self._commits = None
        self._jobs = None
        self._table_reader = None
        self._search_repository = None
        self._explorations = None
    
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
            # Release based on connection type
            if self._is_adapted and hasattr(self._connection, 'raw_connection'):
                await self._pool._pool.release(self._connection.raw_connection)
            elif isinstance(self._connection, asyncpg.Connection):
                await self._pool._pool.release(self._connection)
            self._connection = None
            self._is_adapted = False
        
        # Reset repositories
        self._users = None
        self._datasets = None
        self._commits = None
        self._jobs = None
        self._table_reader = None
        self._search_repository = None
        self._explorations = None
    
    async def begin(self):
        """Begin a new transaction."""
        if self._connection is None:
            raw_conn = await self._pool._pool.acquire()
            # Wrap in adapter for consistency
            self._connection = AsyncpgConnectionAdapter(raw_conn)
            self._is_adapted = True
            # Get transaction from raw connection
            self._transaction = raw_conn.transaction()
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
    def connection(self):
        """Get the current database connection."""
        if not self._connection:
            raise RuntimeError("No active connection in unit of work")
        return self._connection
    
    @property
    def users(self):
        """Get the user repository."""
        if not self._users:
            self._users = PostgresUserRepository(self.connection)
        return self._users
    
    @property
    def datasets(self):
        """Get the dataset repository."""
        if not self._datasets:
            self._datasets = PostgresDatasetRepository(self.connection)
        return self._datasets
    
    @property
    def commits(self):
        """Get the commit repository."""
        if not self._commits:
            self._commits = PostgresCommitRepository(self.connection)
        return self._commits
    
    @property
    def jobs(self):
        """Get the job repository."""
        if not self._jobs:
            self._jobs = PostgresJobRepository(self.connection)
        return self._jobs
    
    @property
    def table_reader(self):
        """Get the table reader."""
        if not self._table_reader:
            self._table_reader = PostgresTableReader(self.connection)
        return self._table_reader
    
    @property
    def search_repository(self):
        """Get the search repository."""
        if not self._search_repository:
            self._search_repository = PostgresSearchRepository(self.connection)
        return self._search_repository
    
    @property
    def explorations(self):
        """Get the exploration repository."""
        if not self._explorations:
            self._explorations = PostgresExplorationRepository(self.connection)
        return self._explorations