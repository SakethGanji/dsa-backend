"""Core abstractions for the DSA platform."""

from .repositories import (
    IUserRepository,
    IDatasetRepository,
    ICommitRepository,
    IJobRepository,
    ITableReader
)
from .table_interfaces import (
    ITableMetadataReader,
    ITableDataReader,
    ITableAnalytics
)
from .commit_interfaces import (
    ICommitOperations,
    IRefOperations,
    IManifestOperations
)
from .uow import IUnitOfWork
from .services import (
    IFileProcessingService,
    IStatisticsService,
    IExplorationService,
    ISamplingService,
    IWorkbenchService
)

__all__ = [
    # Unit of Work
    'IUnitOfWork',
    # Repositories
    'IUserRepository',
    'IDatasetRepository',
    'ICommitRepository',
    'IJobRepository',
    'ITableReader',
    # Table Interfaces (Segregated)
    'ITableMetadataReader',
    'ITableDataReader',
    'ITableAnalytics',
    # Commit Interfaces (Segregated)
    'ICommitOperations',
    'IRefOperations', 
    'IManifestOperations',
    # Services
    'IFileProcessingService',
    'IStatisticsService',
    'IExplorationService',
    'ISamplingService',
    'IWorkbenchService',
]