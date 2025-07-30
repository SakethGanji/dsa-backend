"""Versioning command objects."""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime


@dataclass
class CreateCommitCommand:
    """Command to create a new commit."""
    dataset_id: int
    user_id: int
    branch_name: str
    message: str
    data: List[Dict[str, Any]]
    parent_commit_id: Optional[str] = None
    schema: Optional[Dict[str, Any]] = None


@dataclass
class CheckoutCommitCommand:
    """Command to checkout a specific commit."""
    dataset_id: int
    user_id: int
    commit_id: str
    create_branch: Optional[str] = None


@dataclass
class GetCommitHistoryCommand:
    """Command to get commit history."""
    dataset_id: int
    user_id: int
    ref_name: str = "main"
    offset: int = 0
    limit: int = 50


@dataclass
class GetCommitSchemaCommand:
    """Command to get schema for a commit."""
    dataset_id: int
    user_id: int
    commit_id: str
    table_key: Optional[str] = None


@dataclass
class GetDataAtRefCommand:
    """Command to get data at a specific ref."""
    dataset_id: int
    user_id: int
    ref_name: str
    table_key: str = "primary"
    offset: int = 0
    limit: int = 100


@dataclass
class GetTableDataCommand:
    """Command to get table data from a ref."""
    dataset_id: int
    user_id: int
    ref_name: str
    table_key: str
    offset: int = 0
    limit: int = 500
    order_by: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None


@dataclass
class GetDatasetOverviewCommand:
    """Command to get dataset overview at a ref."""
    dataset_id: int
    user_id: int
    ref_name: str = "main"


@dataclass
class GetTableAnalysisCommand:
    """Command to get table analysis."""
    dataset_id: int
    user_id: int
    ref_name: str
    table_key: str
    use_cache: bool = True
    sample_size: int = 1000


@dataclass
class QueueImportJobCommand:
    """Command to queue a data import job."""
    dataset_id: int
    user_id: int
    file_name: str
    file_size: int
    file_path: str  # Temporary file path
    branch_name: str = "main"
    commit_message: Optional[str] = None
    append_mode: bool = False
    
    def __post_init__(self):
        if not self.commit_message:
            self.commit_message = f"Import {self.file_name}"


@dataclass
class CreateBranchCommand:
    """Command to create a new branch."""
    dataset_id: int
    user_id: int
    branch_name: str
    from_ref: str = "main"


@dataclass
class DeleteBranchCommand:
    """Command to delete a branch."""
    dataset_id: int
    user_id: int
    branch_name: str


@dataclass
class CreateTagCommand:
    """Command to create a new tag."""
    dataset_id: int
    user_id: int
    tag_name: str
    ref_name: str = "main"
    message: Optional[str] = None


@dataclass
class MergeBranchCommand:
    """Command to merge branches."""
    dataset_id: int
    user_id: int
    source_branch: str
    target_branch: str = "main"
    commit_message: Optional[str] = None
    strategy: str = "fast-forward"  # fast-forward, merge, rebase
    
    def __post_init__(self):
        if not self.commit_message:
            self.commit_message = f"Merge {self.source_branch} into {self.target_branch}"


@dataclass
class ListRefsCommand:
    """Command to list refs for a dataset."""
    dataset_id: int
    user_id: int
    ref_type: Optional[str] = None  # Filter by branch or tag