"""Shared exception classes for the application.

This module contains exception classes that can be used across
multiple vertical slices for consistent error handling.
"""
from typing import Any


class CoreException(Exception):
    """Base exception for all application-specific exceptions."""
    pass


class ArtifactException(CoreException):
    """Base exception for artifact-related errors."""
    pass


class ArtifactCreationError(ArtifactException):
    """Raised when artifact creation fails."""
    pass


class ArtifactNotFoundError(ArtifactException):
    """Raised when a requested artifact does not exist."""
    pass


class StorageException(CoreException):
    """Base exception for storage-related errors."""
    pass


class StorageWriteError(StorageException):
    """Raised when writing to storage fails."""
    pass


class StorageReadError(StorageException):
    """Raised when reading from storage fails."""
    pass


class ValidationException(CoreException):
    """Base exception for validation errors."""
    pass


class InvalidFileTypeError(ValidationException):
    """Raised when an invalid file type is provided."""
    pass


class InvalidStreamError(ValidationException):
    """Raised when an invalid stream is provided."""
    pass


class ConcurrencyException(CoreException):
    """Base exception for concurrency-related errors."""
    pass


class RaceConditionError(ConcurrencyException):
    """Raised when a race condition is detected during artifact creation."""
    pass


class EntityException(CoreException):
    """Base exception for entity-related errors."""
    pass


class EntityNotFoundError(EntityException):
    """Raised when an entity is not found."""
    def __init__(self, entity_type: str, entity_id: Any):
        self.entity_type = entity_type
        self.entity_id = entity_id
        super().__init__(f"{entity_type} {entity_id} not found")


class DomainException(CoreException):
    """Base exception for domain-specific errors in vertical slices."""
    pass