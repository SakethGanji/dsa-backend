"""Dataset command objects."""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any


@dataclass
class CreateDatasetCommand:
    """Command to create a new dataset."""
    name: str
    created_by: int
    description: Optional[str] = None
    tags: List[str] = None
    default_branch: str = "main"
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class CreateDatasetWithFileCommand:
    """Command to create a dataset with initial file upload."""
    name: str
    created_by: int
    file_name: str
    file_size: int
    file_content: Any  # File stream or bytes
    description: Optional[str] = None
    tags: List[str] = None
    default_branch: str = "main"
    branch_name: str = "main"
    commit_message: Optional[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.commit_message is None:
            self.commit_message = f"Initial import of {self.file_name}"


@dataclass
class UpdateDatasetCommand:
    """Command to update dataset information."""
    dataset_id: int
    user_id: int
    name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None


@dataclass
class DeleteDatasetCommand:
    """Command to delete a dataset."""
    dataset_id: int
    user_id: int
    force: bool = False


@dataclass
class GrantPermissionCommand:
    """Command to grant dataset permission."""
    dataset_id: int
    user_id: int  # User performing the grant
    target_user_id: int  # User receiving the permission
    permission_type: str  # read, write, or admin


@dataclass
class RevokePermissionCommand:
    """Command to revoke dataset permission."""
    dataset_id: int
    user_id: int  # User performing the revoke
    target_user_id: int  # User losing the permission
    permission_type: str  # read, write, or admin


@dataclass
class GetDatasetCommand:
    """Command to get dataset details."""
    dataset_id: int
    user_id: int
    include_stats: bool = True
    include_permissions: bool = False


@dataclass
class ListDatasetsCommand:
    """Command to list datasets."""
    user_id: int
    offset: int = 0
    limit: int = 100
    name_filter: Optional[str] = None
    tag_filter: Optional[List[str]] = None
    owned_only: bool = False
    include_stats: bool = True


@dataclass
class CheckDatasetReadyCommand:
    """Command to check if dataset is ready for operations."""
    dataset_id: int
    user_id: int