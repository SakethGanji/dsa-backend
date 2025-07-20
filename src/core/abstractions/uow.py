"""Unit of Work pattern interface."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .repositories import (
        IUserRepository,
        IDatasetRepository,
        ICommitRepository,
        IJobRepository,
        ITableReader,
        IExplorationRepository
    )
    from .search_repository import ISearchRepository


class IUnitOfWork(ABC):
    """Manages database transactions"""
    @abstractmethod
    async def begin(self) -> None:
        pass
    
    @abstractmethod
    async def commit(self) -> None:
        pass
    
    @abstractmethod
    async def rollback(self) -> None:
        pass
    
    @property
    @abstractmethod
    def connection(self):
        """Get the database connection"""
        pass
    
    @property
    @abstractmethod
    def users(self) -> 'IUserRepository':
        """Get the user repository"""
        pass
    
    @property
    @abstractmethod
    def datasets(self) -> 'IDatasetRepository':
        """Get the dataset repository"""
        pass
    
    @property
    @abstractmethod
    def commits(self) -> 'ICommitRepository':
        """Get the commit repository"""
        pass
    
    @property
    @abstractmethod
    def jobs(self) -> 'IJobRepository':
        """Get the job repository"""
        pass
    
    @property
    @abstractmethod
    def table_reader(self) -> 'ITableReader':
        """Get the table reader"""
        pass
    
    @property
    @abstractmethod
    def search_repository(self) -> 'ISearchRepository':
        """Get the search repository"""
        pass
    
    @property
    @abstractmethod
    def explorations(self) -> 'IExplorationRepository':
        """Get the exploration repository"""
        pass