"""Core abstractions for the DSA platform."""

from .repositories import (
    IUserRepository,
    IDatasetRepository,
    ICommitRepository,
    IJobRepository,
    ITableReader,
    IExplorationRepository
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
    IWorkbenchService,
    ITableAnalysisService,
    IDataTypeInferenceService,
    IColumnStatisticsService,
    ISqlValidationService,
    ISqlExecutionService,
    IQueryOptimizationService,
    IDataExportService,
    ICommitPreparationService,
    ExportOptions,
    ExportResult,
    CommitData
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
    'IExplorationRepository',
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
    'ITableAnalysisService',
    'IDataTypeInferenceService',
    'IColumnStatisticsService',
    'ISqlValidationService',
    'ISqlExecutionService',
    'IQueryOptimizationService',
    'IDataExportService',
    'ICommitPreparationService',
    'ExportOptions',
    'ExportResult',
    'CommitData',
]

# Import event abstractions separately to avoid circular imports
from .events import (
    IEventBus,
    IEventStore,
    IEventHandler,
    IEventPublisher,
    DomainEvent,
    EventType,
    DatasetCreatedEvent,
    DatasetUpdatedEvent,
    DatasetDeletedEvent,
    CommitCreatedEvent,
    UserCreatedEvent,
    UserUpdatedEvent,
    UserDeletedEvent,
    JobCreatedEvent,
    JobStartedEvent,
    JobCompletedEvent,
    JobFailedEvent,
    JobCancelledEvent,
    JobLifecycleEvent,
    PermissionGrantedEvent,
    BranchDeletedEvent
)

__all__.extend([
    # Event abstractions
    'IEventBus',
    'IEventStore',
    'IEventHandler',
    'IEventPublisher',
    'DomainEvent',
    'EventType',
    'DatasetCreatedEvent',
    'DatasetUpdatedEvent',
    'DatasetDeletedEvent',
    'CommitCreatedEvent',
    'UserCreatedEvent',
    'UserUpdatedEvent',
    'UserDeletedEvent',
    'JobCreatedEvent',
    'JobStartedEvent',
    'JobCompletedEvent',
    'JobFailedEvent',
    'JobCancelledEvent',
    'JobLifecycleEvent',
    'PermissionGrantedEvent',
    'BranchDeletedEvent',
])