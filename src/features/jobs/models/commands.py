"""Job command objects."""

from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from uuid import UUID
from datetime import datetime


@dataclass
class CreateJobCommand:
    """Command to create a new job."""
    user_id: int  # Must be first for decorator
    dataset_id: int
    run_type: str
    source_commit_id: str
    run_parameters: Dict[str, Any]
    description: Optional[str] = None


@dataclass
class GetJobCommand:
    """Command to get job details."""
    job_id: UUID
    user_id: int


@dataclass
class GetJobStatusCommand:
    """Command to get job status."""
    job_id: UUID
    user_id: int


@dataclass
class CancelJobCommand:
    """Command to cancel a job."""
    user_id: int  # Must be first for decorator
    job_id: UUID
    dataset_id: int  # For permission check


@dataclass
class GetJobsCommand:
    """Command to list jobs."""
    user_id: int
    dataset_id: Optional[int] = None
    job_type: Optional[str] = None
    status: Optional[str] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    offset: int = 0
    limit: int = 20
    order_by: str = "created_at"
    order_desc: bool = True


@dataclass
class RetryJobCommand:
    """Command to retry a failed job."""
    job_id: UUID
    user_id: int