"""Repository interfaces for data access layer."""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Tuple, Set, AsyncGenerator
from datetime import datetime
from uuid import UUID


class IUserRepository(ABC):
    """User management operations"""
    @abstractmethod
    async def get_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        pass
    
    @abstractmethod
    async def get_by_soeid(self, soeid: str) -> Optional[Dict[str, Any]]:
        pass
    
    @abstractmethod
    async def create_user(self, soeid: str, password_hash: str, role_id: int) -> int:
        pass
    
    @abstractmethod
    async def get_user_with_password(self, soeid: str) -> Optional[Dict[str, Any]]:
        """Get user including password hash for authentication"""
        pass
    
    @abstractmethod
    async def update_user_password(self, user_id: int, new_password_hash: str) -> None:
        pass


class IDatasetRepository(ABC):
    """Dataset and permission management"""
    @abstractmethod
    async def create_dataset(self, name: str, description: str, created_by: int) -> int:
        pass
    
    @abstractmethod
    async def get_dataset_by_id(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        pass
    
    @abstractmethod
    async def check_user_permission(self, dataset_id: int, user_id: int, required_permission: str) -> bool:
        pass
    
    @abstractmethod
    async def user_has_permission(self, dataset_id: int, user_id: int, permission_type: str) -> bool:
        """Check if user has specific permission on dataset"""
        pass
    
    @abstractmethod
    async def grant_permission(self, dataset_id: int, user_id: int, permission_type: str) -> None:
        pass
    
    @abstractmethod
    async def add_dataset_tags(self, dataset_id: int, tags: List[str]) -> None:
        """Add tags to a dataset"""
        pass
    
    @abstractmethod
    async def get_dataset_tags(self, dataset_id: int) -> List[str]:
        """Get all tags for a dataset"""
        pass
    
    @abstractmethod
    async def update_dataset(self, dataset_id: int, name: Optional[str] = None, 
                           description: Optional[str] = None) -> None:
        """Update dataset metadata"""
        pass
    
    @abstractmethod
    async def delete_dataset(self, dataset_id: int) -> None:
        """Delete a dataset and all its related data"""
        pass
    
    @abstractmethod
    async def remove_dataset_tags(self, dataset_id: int) -> None:
        """Remove all tags from a dataset"""
        pass


class ICommitRepository(ABC):
    """Versioning engine operations"""
    @abstractmethod
    async def add_rows_if_not_exist(self, rows: Set[Tuple[str, str]]) -> None:
        """Add (row_hash, row_data_json) pairs to rows table"""
        pass
    
    @abstractmethod
    async def create_commit_and_manifest(
        self, 
        dataset_id: int,
        parent_commit_id: Optional[str],
        message: str,
        author_id: int,
        manifest: List[Tuple[str, str]]  # List of (logical_row_id, row_hash)
    ) -> str:
        """Create a new commit with its manifest"""
        pass
    
    @abstractmethod
    async def update_ref_atomically(self, dataset_id: int, ref_name: str, new_commit_id: str, expected_commit_id: str) -> bool:
        """Update ref only if it currently points to expected_commit_id"""
        pass
    
    @abstractmethod
    async def get_current_commit_for_ref(self, dataset_id: int, ref_name: str) -> Optional[str]:
        pass
    
    @abstractmethod
    async def get_ref(self, dataset_id: int, ref_name: str) -> Optional[Dict[str, Any]]:
        """Get ref details including commit_id"""
        pass
    
    @abstractmethod
    async def get_commit_data(self, commit_id: str, table_key: Optional[str] = None, offset: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve data for a commit, optionally filtered by table
        
        DEPRECATED: Use ITableReader.get_table_data() instead for consistent table-aware data access.
        This method will be removed in a future version.
        """
        pass
    
    @abstractmethod
    async def create_commit_schema(self, commit_id: str, schema_definition: Dict[str, Any]) -> None:
        pass
    
    @abstractmethod
    async def get_commit_schema(self, commit_id: str) -> Optional[Dict[str, Any]]:
        pass
    
    @abstractmethod
    async def create_commit_statistics(self, commit_id: str, statistics: Dict[str, Any]) -> None:
        """Store statistics for a commit"""
        pass
    
    @abstractmethod
    async def get_commit_history(self, dataset_id: int, ref_name: str = "main", offset: int = 0, limit: int = 50) -> List[Dict[str, Any]]:
        """Get the commit history for a specific ref with pagination."""
        pass
    
    @abstractmethod
    async def get_commit_by_id(self, commit_id: str) -> Optional[Dict[str, Any]]:
        """Get commit details including author info."""
        pass
    
    @abstractmethod
    async def count_commits_for_dataset(self, dataset_id: int, ref_name: str = "main") -> int:
        """Count total commits for a dataset starting from a specific ref."""
        pass
    
    @abstractmethod
    async def count_commit_rows(self, commit_id: str, table_key: Optional[str] = None) -> int:
        """Count rows in a commit, optionally filtered by table."""
        pass
    
    @abstractmethod
    async def list_refs(self, dataset_id: int) -> List[Dict[str, Any]]:
        """List all refs/branches for a dataset."""
        pass
    
    @abstractmethod
    async def create_ref(self, dataset_id: int, ref_name: str, commit_id: str) -> None:
        """Create a new ref pointing to a specific commit."""
        pass
    
    @abstractmethod
    async def delete_ref(self, dataset_id: int, ref_name: str) -> bool:
        """Delete a ref. Returns True if deleted, False if not found."""
        pass
    
    @abstractmethod
    async def get_default_branch(self, dataset_id: int) -> Optional[str]:
        """Get the default branch name for a dataset (usually 'main')."""
        pass


class IJobRepository(ABC):
    """Job queue management"""
    @abstractmethod
    async def create_job(
        self,
        run_type: str,
        dataset_id: int,
        user_id: int,
        source_commit_id: Optional[str] = None,
        run_parameters: Optional[Dict[str, Any]] = None
    ) -> UUID:
        pass
    
    @abstractmethod
    async def acquire_next_pending_job(self, job_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Atomically acquire the next pending job for processing"""
        pass
    
    @abstractmethod
    async def update_job_status(
        self,
        job_id: UUID,
        status: str,
        output_summary: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> None:
        pass
    
    @abstractmethod
    async def get_job_by_id(self, job_id: UUID) -> Optional[Dict[str, Any]]:
        pass
    
    @abstractmethod
    async def get_sampling_jobs_by_dataset(
        self,
        dataset_id: int,
        ref_name: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get sampling jobs for a dataset with optional filters."""
        pass
    
    @abstractmethod
    async def get_sampling_jobs_by_user(
        self,
        user_id: int,
        dataset_id: Optional[int] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get sampling jobs created by a user with optional filters."""
        pass


class ITableReader(ABC):
    """
    Interface for reading data and metadata from specific tables within versioned commits.
    
    This abstraction provides a clean way to access table data regardless of the
    underlying storage mechanism or file format (Parquet, CSV, Excel).
    """
    
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
    async def get_column_samples(
        self, 
        commit_id: str, 
        table_key: str, 
        columns: List[str], 
        samples_per_column: int = 20
    ) -> Dict[str, List[Any]]:
        """
        Get unique sample values per column using SQL.
        
        Args:
            commit_id: The commit ID
            table_key: The table key
            columns: List of column names to sample
            samples_per_column: Number of unique values to sample per column
            
        Returns:
            Dict mapping column names to lists of sample values
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