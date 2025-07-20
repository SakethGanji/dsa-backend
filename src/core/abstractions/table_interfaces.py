"""Segregated interfaces for table operations following Interface Segregation Principle."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, AsyncGenerator


class ITableMetadataReader(ABC):
    """Read-only interface for table metadata operations."""
    
    @abstractmethod
    async def list_table_keys(self, commit_id: str) -> List[str]:
        """
        List all available table keys for a given commit.
        
        Returns:
            - For Parquet/CSV: ['primary']
            - For single-sheet Excel: ['Sheet1'] 
            - For multi-sheet Excel: ['Revenue', 'Expenses', ...]
        """
        pass
    
    @abstractmethod
    async def get_table_schema(self, commit_id: str, table_key: str) -> Optional[Dict[str, Any]]:
        """
        Get the schema for a specific table within a commit.
        
        Args:
            commit_id: The commit ID
            table_key: The table key (e.g., 'primary', 'Revenue')
            
        Returns:
            Schema dict with columns and metadata, or None if table not found
        """
        pass
    
    @abstractmethod
    async def count_table_rows(self, commit_id: str, table_key: str) -> int:
        """
        Get the total row count for a specific table.
        
        Args:
            commit_id: The commit ID
            table_key: The table key
            
        Returns:
            Total number of rows in the table
        """
        pass
    
    @abstractmethod
    async def batch_get_table_metadata(self, commit_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Batch fetch table metadata for multiple commits in a single operation.
        
        Args:
            commit_ids: List of commit IDs to fetch metadata for
            
        Returns:
            Dict mapping commit_id to list of table metadata dicts containing:
            - table_key: The table identifier
            - row_count: Number of rows
            - column_count: Number of columns
            - created_at: Creation timestamp
            - Any other metadata
        """
        pass


class ITableDataReader(ABC):
    """Interface for reading table data."""
    
    @abstractmethod
    async def get_table_data(
        self,
        commit_id: str,
        table_key: str,
        offset: int = 0,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get paginated data for a specific table.
        
        Args:
            commit_id: The commit ID
            table_key: The table key
            offset: Number of rows to skip
            limit: Maximum rows to return (None for all)
            
        Returns:
            List of row data dictionaries
        """
        pass
    
    @abstractmethod
    def get_table_data_stream(
        self,
        commit_id: str,
        table_key: str,
        batch_size: int = 1000
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Stream data for a specific table in batches.
        Ideal for processing large datasets without loading all into memory.
        
        Args:
            commit_id: The commit ID
            table_key: The table key
            batch_size: Number of rows per batch
            
        Yields:
            Batches of row data dictionaries
        """
        pass
    
    @abstractmethod
    async def get_column_samples(
        self, 
        commit_id: str, 
        table_key: str, 
        columns: List[str], 
        samples_per_column: int = 20
    ) -> Dict[str, List[Any]]:
        """
        Get unique sample values per column.
        
        Args:
            commit_id: The commit ID
            table_key: The table key
            columns: List of column names to sample
            samples_per_column: Number of unique values to sample per column
            
        Returns:
            Dict mapping column names to lists of sample values
        """
        pass


class ITableAnalytics(ABC):
    """Interface for table analytics and statistics operations."""
    
    @abstractmethod
    async def get_table_statistics(self, commit_id: str, table_key: str) -> Optional[Dict[str, Any]]:
        """
        Get statistics for a specific table within a commit.
        
        Args:
            commit_id: The commit ID
            table_key: The table key
            
        Returns:
            Statistics dict with row counts, null counts, etc., or None if not found
        """
        pass
    
    @abstractmethod
    def get_table_sample_stream(
        self, 
        commit_id: str, 
        table_key: str,
        sample_method: str, 
        sample_params: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Stream sampled data based on method.
        
        Args:
            commit_id: The commit ID
            table_key: The table key
            sample_method: Sampling method ('random', 'stratified', 'systematic', 'cluster')
            sample_params: Parameters specific to the sampling method
            
        Yields:
            Sampled row data dictionaries
        """
        pass