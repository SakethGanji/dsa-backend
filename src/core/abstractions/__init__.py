"""Core abstractions for the DSA platform."""

from .repositories import (
    IUserRepository,
    IDatasetRepository,
    ICommitRepository,
    IJobRepository,
    ITableReader
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
    # Services
    'IFileProcessingService',
    'IStatisticsService',
    'IExplorationService',
    'ISamplingService',
    'IWorkbenchService',
]