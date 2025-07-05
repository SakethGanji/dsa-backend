from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Tuple, Set
from datetime import datetime
from uuid import UUID


class IUnitOfWork(ABC):
    """Manages database transactions"""
    @abstractmethod
    async def begin(self) -> None:
        pass
    
    @abstractmethod
    async def commit(self) -> None:
        pass
    
    @abstractmethod
    async def rollback(self) -> None:
        pass


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
    async def grant_permission(self, dataset_id: int, user_id: int, permission_type: str) -> None:
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
    async def get_commit_data(self, commit_id: str, sheet_name: Optional[str] = None, offset: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve data for a commit, optionally filtered by sheet"""
        pass
    
    @abstractmethod
    async def create_commit_schema(self, commit_id: str, schema_definition: Dict[str, Any]) -> None:
        pass
    
    @abstractmethod
    async def get_commit_schema(self, commit_id: str) -> Optional[Dict[str, Any]]:
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