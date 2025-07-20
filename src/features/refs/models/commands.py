"""Refs command objects."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class CreateBranchCommand:
    """Command to create a new branch."""
    user_id: int  # Must be first for decorator
    dataset_id: int
    ref_name: str
    from_ref: str = "main"  # Source branch/ref


@dataclass
class DeleteBranchCommand:
    """Command to delete a branch."""
    user_id: int  # Must be first for decorator
    dataset_id: int
    ref_name: str


@dataclass
class ListRefsCommand:
    """Command to list refs for a dataset."""
    user_id: int  # Must be first for decorator
    dataset_id: int
    ref_type: Optional[str] = None  # Filter by branch or tag