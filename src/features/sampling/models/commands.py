"""Sampling command objects."""

from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime
from uuid import UUID


@dataclass
class CreateSamplingJobCommand:
    """Command to create a complex sampling job with multiple rounds."""
    user_id: int
    dataset_id: int
    source_ref: str
    table_key: str
    output_branch_name: Optional[str]
    output_name: Optional[str]
    commit_message: str
    rounds: List[Dict[str, Any]]
    export_residual: bool
    residual_output_name: str




@dataclass
class GetSamplingHistoryCommand:
    """Command to get sampling job history."""
    user_id: int
    dataset_id: int
    ref_name: Optional[str] = None
    status: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    offset: int = 0
    limit: int = 20


@dataclass
class GetJobDataCommand:
    """Command to get sampling job data."""
    user_id: int
    job_id: UUID
    format: str = "json"  # json, csv
    offset: int = 0
    limit: Optional[int] = None




@dataclass
class GetSamplingMethodsCommand:
    """Command to get available sampling methods."""
    user_id: int