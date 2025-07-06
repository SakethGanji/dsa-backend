"""PostgreSQL Unit of Work implementation."""

from typing import Optional
from asyncpg import Connection

from ...abstractions import IUnitOfWork
from .user_repo import PostgresUserRepository
from .dataset_repo import PostgresDatasetRepository
from .versioning_repo import PostgresCommitRepository
from .job_repo import PostgresJobRepository
from .table_reader import PostgresTableReader
from .search_repository import PostgresSearchRepository


class PostgresUnitOfWork(IUnitOfWork):
    """PostgreSQL implementation of Unit of Work pattern."""
    
    def __init__(self, pool):
        self._pool = pool
        self._connection: Optional[Connection] = None
        self._transaction = None
        self._users = None
        self._datasets = None
        self._commits = None
        self._jobs = None
        self._table_reader = None
        self._search_repository = None
    
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
        
        # Reset repositories
        self._users = None
        self._datasets = None
        self._commits = None
        self._jobs = None
        self._table_reader = None
        self._search_repository = None
    
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