"""Dependency injection configuration for the application.

This module configures dependency injection for all services, repositories,
and other components, ensuring loose coupling and testability.
"""

from typing import Annotated, Any
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.interfaces import IArtifactProducer
from app.storage.interfaces import IStorageBackend, IStorageFactory
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
    # TODO: Get backend type from configuration
    # For now, hardcode to local backend
    return factory.create_backend("local")


# Core dependencies

def get_artifact_producer(
    db: Annotated[AsyncSession, Depends(get_session)],
    storage: Annotated[IStorageBackend, Depends(get_storage_backend)]
) -> IArtifactProducer:
    """Get artifact producer instance.
    
    Args:
        db: Database session for transactional operations.
        storage: Storage backend for file operations.
    
    Returns:
        An instance of IArtifactProducer for centralized file creation.
    """
    from app.storage.services import ArtifactProducer
    return ArtifactProducer(db, storage)


# Dataset dependencies

def get_dataset_repository(
    db: Annotated[AsyncSession, Depends(get_session)]
):
    """Get dataset repository instance.
    
    Args:
        db: Database session.
    
    Returns:
        An instance of DatasetRepository.
    """
    from app.datasets.repository import DatasetRepository
    return DatasetRepository(db)


def get_dataset_service(
    repository: Annotated[Any, Depends(get_dataset_repository)],
    artifact_producer: Annotated[IArtifactProducer, Depends(get_artifact_producer)]
):
    """Get dataset service instance.
    
    Args:
        repository: Dataset repository for data access.
        artifact_producer: Artifact producer for file creation.
    
    Returns:
        An instance of DatasetService.
    """
    from app.datasets.service import DatasetService
    return DatasetService(repository, artifact_producer)


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