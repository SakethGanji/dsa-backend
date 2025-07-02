"""Core module for shared interfaces, types, and utilities.

This module provides the foundation for clean architecture by defining
interfaces, types, and exceptions that are shared across vertical slices.
"""

from .interfaces import IArtifactProducer, FileId
from .types import (
    DatasetId,
    VersionId,
    StoragePath,
    UserId,
    JobId,
    SamplingJobId,
    AnalysisJobId,
)
from .exceptions import (
    CoreException,
    ArtifactException,
    ArtifactCreationError,
    ArtifactNotFoundError,
    StorageException,
    StorageWriteError,
    StorageReadError,
    ValidationException,
    InvalidFileTypeError,
    InvalidStreamError,
    ConcurrencyException,
    RaceConditionError,
)

__all__ = [
    # Interfaces
    "IArtifactProducer",
    # Types
    "FileId",
    "DatasetId",
    "VersionId",
    "StoragePath",
    "UserId",
    "JobId",
    "SamplingJobId",
    "AnalysisJobId",
    # Exceptions
    "CoreException",
    "ArtifactException",
    "ArtifactCreationError",
    "ArtifactNotFoundError",
    "StorageException",
    "StorageWriteError",
    "StorageReadError",
    "ValidationException",
    "InvalidFileTypeError",
    "InvalidStreamError",
    "ConcurrencyException",
    "RaceConditionError",
]