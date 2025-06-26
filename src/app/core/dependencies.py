"""Dependency injection configuration for the application.

This module configures dependency injection for all services, repositories,
and other components, ensuring loose coupling and testability.
"""

from typing import Annotated, Any
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.interfaces import IArtifactProducer
from app.core.events import IEventBus
from app.storage.interfaces import IStorageBackend, IStorageFactory
from app.datasets.interfaces import IDatasetRepository, IDatasetService, IDatasetSearchService
from app.db.connection import get_session


# Storage dependencies

def get_storage_factory() -> IStorageFactory:
    """Get storage factory instance.
    
    Returns:
        An instance of IStorageFactory for creating storage backends.
    """
    from app.storage.factory import StorageFactory
    return StorageFactory()


def get_storage_backend(
    factory: Annotated[IStorageFactory, Depends(get_storage_factory)]
) -> IStorageBackend:
    """Get storage backend instance.
    
    Args:
        factory: The storage factory to use for creating backends.
    
    Returns:
        An instance of IStorageBackend configured for the application.
    """
    import os
    # Get backend type from environment variable, default to "local"
    backend_type = os.getenv("STORAGE_BACKEND", "local")
    return factory.create_backend(backend_type)


# Event bus dependency

_event_bus_instance = None

def get_event_bus() -> IEventBus:
    """Get event bus instance.
    
    Returns:
        An instance of IEventBus for cross-slice communication.
        In production, this could return a Redis-backed event bus.
    """
    global _event_bus_instance
    if _event_bus_instance is None:
        from app.core.services.event_bus import InMemoryEventBus
        _event_bus_instance = InMemoryEventBus()
    return _event_bus_instance


# Core dependencies

def get_artifact_producer(
    db: Annotated[AsyncSession, Depends(get_session)],
    storage: Annotated[IStorageBackend, Depends(get_storage_backend)],
    event_bus: Annotated[IEventBus, Depends(get_event_bus)]
) -> IArtifactProducer:
    """Get artifact producer instance.
    
    Args:
        db: Database session for transactional operations.
        storage: Storage backend for file operations.
        event_bus: Event bus for publishing events.
    
    Returns:
        An instance of IArtifactProducer for centralized file creation.
    """
    from app.storage.services import ArtifactProducer
    return ArtifactProducer(db, storage, event_bus)


# Dataset dependencies

def get_dataset_repository(
    db: Annotated[AsyncSession, Depends(get_session)]
) -> IDatasetRepository:
    """Get dataset repository instance.
    
    Args:
        db: Database session.
    
    Returns:
        An instance of IDatasetRepository.
    """
    from app.datasets.repository import DatasetsRepository
    return DatasetsRepository(db)


def get_dataset_service(
    repository: Annotated[IDatasetRepository, Depends(get_dataset_repository)],
    storage_backend: Annotated[IStorageBackend, Depends(get_storage_backend)],
    artifact_producer: Annotated[IArtifactProducer, Depends(get_artifact_producer)],
    event_bus: Annotated[IEventBus, Depends(get_event_bus)],
    db: Annotated[AsyncSession, Depends(get_session)]
) -> IDatasetService:
    """Get dataset service instance.
    
    Args:
        repository: Dataset repository for data access.
        storage_backend: Storage backend for file operations.
        artifact_producer: Artifact producer for file creation.
        event_bus: Event bus for cross-slice communication.
        db: Database session.
    
    Returns:
        An instance of IDatasetService.
    """
    from app.datasets.service import DatasetsService
    from app.users.service import UserService
    
    # Create user service instance
    user_service = UserService(db)
    
    return DatasetsService(
        repository=repository, 
        storage_backend=storage_backend,
        user_service=user_service,
        artifact_producer=artifact_producer,
        event_bus=event_bus
    )


def get_dataset_search_service(
    repository: Annotated[IDatasetRepository, Depends(get_dataset_repository)]
) -> IDatasetSearchService:
    """Get dataset search service instance.
    
    Args:
        repository: Dataset repository for data access.
    
    Returns:
        An instance of IDatasetSearchService.
    """
    from app.datasets.search.service import DatasetSearchService
    return DatasetSearchService(repository)


# Sampling dependencies

def get_sampling_repository(
    db: Annotated[AsyncSession, Depends(get_session)]
):
    """Get sampling repository instance.
    
    Args:
        db: Database session.
    
    Returns:
        An instance of sampling repository.
    """
    from app.sampling.db_repository import SamplingDBRepository
    return SamplingDBRepository(db)


def get_sampling_service(
    repository: Annotated[Any, Depends(get_sampling_repository)],
    artifact_producer: Annotated[IArtifactProducer, Depends(get_artifact_producer)]
):
    """Get sampling service instance.
    
    Args:
        repository: Sampling repository for data access.
        artifact_producer: Artifact producer for file creation.
    
    Returns:
        An instance of SamplingService.
    """
    from app.sampling.service import SamplingService
    return SamplingService(repository, artifact_producer)


# Analysis dependencies
# Note: Since analysis functionality is merged with sampling in the new schema,
# these would likely be refactored to use the same service/repository