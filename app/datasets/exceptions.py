"""Custom exceptions for the datasets slice"""
from app.core.exceptions import DomainException, EntityNotFoundError, StorageException


class DatasetException(DomainException):
    """Base exception for dataset-related errors"""
    pass


class DatasetNotFound(EntityNotFoundError):
    """Raised when a dataset is not found"""
    def __init__(self, dataset_id: int):
        super().__init__("Dataset", dataset_id)
        self.dataset_id = dataset_id


class DatasetVersionNotFound(EntityNotFoundError):
    """Raised when a dataset version is not found"""
    def __init__(self, version_id: int):
        super().__init__("Dataset version", version_id)
        self.version_id = version_id


class FileProcessingError(DatasetException):
    """Raised when file processing fails"""
    def __init__(self, filename: str, error: str):
        self.filename = filename
        self.error = error
        super().__init__(f"Error processing file {filename}: {error}")


class DatasetStorageError(StorageException):
    """Raised when dataset storage operations fail"""
    def __init__(self, operation: str, error: str):
        self.operation = operation
        self.error = error
        super().__init__(f"Storage error during {operation}: {error}")