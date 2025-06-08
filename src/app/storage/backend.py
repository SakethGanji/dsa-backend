"""Storage backend abstraction layer."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Protocol
from pathlib import Path
import pandas as pd
import duckdb


class DatasetReader(Protocol):
    """Protocol for reading dataset data in various formats."""
    
    def to_pandas(self) -> pd.DataFrame:
        """Read dataset as pandas DataFrame."""
        ...
    
    def to_duckdb(self, conn: duckdb.DuckDBPyConnection, view_name: str = "main_data") -> None:
        """Create a DuckDB view from the dataset."""
        ...
    
    def get_path(self) -> str:
        """Get the path/URI of the dataset."""
        ...
    
    def read_with_selection(
        self,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Read dataset with column selection and pagination."""
        ...


class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def read_dataset(self, dataset_id: int, version_id: int, file_path: str) -> DatasetReader:
        """Read a dataset by its ID and version.
        
        Args:
            dataset_id: The dataset identifier
            version_id: The version identifier
            file_path: The file path from database
            
        Returns:
            DatasetReader instance for accessing the data
        """
        pass
    
    @abstractmethod
    def save_sample(
        self, 
        conn: duckdb.DuckDBPyConnection,
        query: str,
        dataset_id: int,
        sample_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Save a sample from a DuckDB query.
        
        Args:
            conn: DuckDB connection with the data
            query: SQL query to execute for the sample
            dataset_id: The dataset identifier
            sample_id: Unique identifier for the sample
            metadata: Optional metadata to store with the sample
            
        Returns:
            Dictionary with sample information (path, size, etc.)
        """
        pass
    
    @abstractmethod
    def list_samples(self, dataset_id: int) -> List[Dict[str, Any]]:
        """List all samples for a dataset.
        
        Args:
            dataset_id: The dataset identifier
            
        Returns:
            List of sample information dictionaries
        """
        pass
    
    @abstractmethod
    def delete_sample(self, dataset_id: int, sample_id: str) -> bool:
        """Delete a sample.
        
        Args:
            dataset_id: The dataset identifier
            sample_id: The sample identifier
            
        Returns:
            True if deleted successfully, False otherwise
        """
        pass
    
    @abstractmethod
    def get_sample_path(self, dataset_id: int, sample_id: str) -> str:
        """Get the path/URI for a sample.
        
        Args:
            dataset_id: The dataset identifier
            sample_id: The sample identifier
            
        Returns:
            Path or URI to the sample
        """
        pass
    
    @abstractmethod
    def get_sample_save_path(self, dataset_id: int, version_id: int, job_id: str) -> str:
        """Get the path where a sample should be saved.
        
        Args:
            dataset_id: The dataset identifier
            version_id: The version identifier
            job_id: The sampling job identifier
            
        Returns:
            Path where the sample should be saved
        """
        pass
    
    @abstractmethod
    def ensure_directories(self) -> None:
        """Ensure required directories exist."""
        pass
    
    @abstractmethod
    async def save_dataset_file(
        self,
        file_content: bytes,
        dataset_id: int,
        version_id: int,
        file_name: str
    ) -> Dict[str, Any]:
        """Save a dataset file.
        
        Args:
            file_content: The file content as bytes
            dataset_id: The dataset identifier
            version_id: The version identifier
            file_name: Original filename
            
        Returns:
            Dictionary with file information (path, size, etc.)
        """
        pass
    
    @abstractmethod
    def get_file_metadata(self, file_path: str) -> Dict[str, Any]:
        """Get metadata about a file without loading the data.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with file metadata including columns, types, row count, etc.
        """
        pass