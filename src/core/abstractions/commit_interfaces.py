"""Segregated interfaces for commit/versioning operations following Interface Segregation Principle."""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Tuple, Set
from datetime import datetime


class ICommitOperations(ABC):
    """Interface for core commit operations."""
    
    @abstractmethod
    async def create_commit(
        self, 
        dataset_id: int,
        parent_commit_id: Optional[str],
        message: str,
        author_id: int
    ) -> str:
        """
        Create a new commit.
        
        Args:
            dataset_id: The dataset ID
            parent_commit_id: Parent commit ID (None for initial commit)
            message: Commit message
            author_id: ID of the commit author
            
        Returns:
            The new commit ID
        """
        pass
    
    @abstractmethod
    async def get_commit_by_id(self, commit_id: str) -> Optional[Dict[str, Any]]:
        """
        Get commit details including author info.
        
        Args:
            commit_id: The commit ID
            
        Returns:
            Commit details or None if not found
        """
        pass
    
    @abstractmethod
    async def get_commit_history(
        self, 
        dataset_id: int, 
        ref_name: str = "main", 
        offset: int = 0, 
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get the commit history for a specific ref with pagination.
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref/branch name
            offset: Number of commits to skip
            limit: Maximum commits to return
            
        Returns:
            List of commit details
        """
        pass
    
    @abstractmethod
    async def count_commits_for_dataset(self, dataset_id: int, ref_name: str = "main") -> int:
        """
        Count total commits for a dataset starting from a specific ref.
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref/branch name
            
        Returns:
            Total number of commits
        """
        pass
    
    @abstractmethod
    async def create_commit_schema(self, commit_id: str, schema_definition: Dict[str, Any]) -> None:
        """
        Store schema information for a commit.
        
        Args:
            commit_id: The commit ID
            schema_definition: Schema definition to store
        """
        pass
    
    @abstractmethod
    async def get_commit_schema(self, commit_id: str) -> Optional[Dict[str, Any]]:
        """
        Get schema information for a commit.
        
        Args:
            commit_id: The commit ID
            
        Returns:
            Schema definition or None if not found
        """
        pass


class IRefOperations(ABC):
    """Interface for ref/branch operations."""
    
    @abstractmethod
    async def get_ref(self, dataset_id: int, ref_name: str) -> Optional[Dict[str, Any]]:
        """
        Get ref details including commit_id.
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref/branch name
            
        Returns:
            Ref details or None if not found
        """
        pass
    
    @abstractmethod
    async def create_ref(self, dataset_id: int, ref_name: str, commit_id: str) -> None:
        """
        Create a new ref pointing to a specific commit.
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref/branch name
            commit_id: The commit ID to point to
        """
        pass
    
    @abstractmethod
    async def update_ref_atomically(
        self, 
        dataset_id: int, 
        ref_name: str, 
        new_commit_id: str, 
        expected_commit_id: Optional[str]
    ) -> bool:
        """
        Update ref only if it currently points to expected_commit_id.
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref/branch name
            new_commit_id: New commit ID to point to
            expected_commit_id: Expected current commit ID (None for new refs)
            
        Returns:
            True if updated, False if current commit doesn't match expected
        """
        pass
    
    @abstractmethod
    async def delete_ref(self, dataset_id: int, ref_name: str) -> bool:
        """
        Delete a ref.
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref/branch name
            
        Returns:
            True if deleted, False if not found
        """
        pass
    
    @abstractmethod
    async def list_refs(self, dataset_id: int) -> List[Dict[str, Any]]:
        """
        List all refs/branches for a dataset.
        
        Args:
            dataset_id: The dataset ID
            
        Returns:
            List of ref details
        """
        pass
    
    @abstractmethod
    async def get_current_commit_for_ref(self, dataset_id: int, ref_name: str) -> Optional[str]:
        """
        Get the current commit ID for a ref.
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref/branch name
            
        Returns:
            Current commit ID or None if ref not found
        """
        pass
    
    @abstractmethod
    async def get_default_branch(self, dataset_id: int) -> Optional[str]:
        """
        Get the default branch name for a dataset (usually 'main').
        
        Args:
            dataset_id: The dataset ID
            
        Returns:
            Default branch name or None
        """
        pass


class IManifestOperations(ABC):
    """Interface for manifest and row storage operations."""
    
    @abstractmethod
    async def add_rows_if_not_exist(self, rows: Set[Tuple[str, str]]) -> None:
        """
        Add (row_hash, row_data_json) pairs to rows table if they don't exist.
        
        Args:
            rows: Set of (row_hash, row_data_json) tuples
        """
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
        """
        Create a new commit with its manifest.
        
        Args:
            dataset_id: The dataset ID
            parent_commit_id: Parent commit ID
            message: Commit message
            author_id: Author ID
            manifest: List of (logical_row_id, row_hash) tuples
            
        Returns:
            New commit ID
        """
        pass
    
    
    @abstractmethod
    async def count_commit_rows(self, commit_id: str, table_key: Optional[str] = None) -> int:
        """
        Count rows in a commit, optionally filtered by table.
        
        Args:
            commit_id: The commit ID
            table_key: Optional table key filter
            
        Returns:
            Number of rows
        """
        pass