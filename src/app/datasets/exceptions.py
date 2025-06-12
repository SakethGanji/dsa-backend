"""Custom exceptions for the datasets slice"""
from typing import Optional


class DatasetException(Exception):
    """Base exception for dataset-related errors"""
    pass


class DatasetNotFound(DatasetException):
    """Raised when a dataset is not found"""
    def __init__(self, dataset_id: int):
        self.dataset_id = dataset_id
        super().__init__(f"Dataset {dataset_id} not found")


class DatasetVersionNotFound(DatasetException):
    """Raised when a dataset version is not found"""
    def __init__(self, version_id: int):
        self.version_id = version_id
        super().__init__(f"Dataset version {version_id} not found")


class FileProcessingError(DatasetException):
    """Raised when file processing fails"""
    def __init__(self, filename: str, error: str):
        self.filename = filename
        self.error = error
        super().__init__(f"Error processing file {filename}: {error}")


class StorageError(DatasetException):
    """Raised when storage operations fail"""
    def __init__(self, operation: str, error: str):
        self.operation = operation
        self.error = error
        super().__init__(f"Storage error during {operation}: {error}")


class SheetNotFound(DatasetException):
    """Raised when a sheet is not found"""
    def __init__(self, sheet_name: str, version_id: int):
        self.sheet_name = sheet_name
        self.version_id = version_id
        super().__init__(f"Sheet '{sheet_name}' not found in version {version_id}")