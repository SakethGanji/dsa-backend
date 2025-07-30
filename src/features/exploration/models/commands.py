"""Exploration command objects."""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from uuid import UUID


@dataclass
class ProfileConfig:
    """Configuration for profiling."""
    minimal: bool = False
    samples_head: int = 10
    samples_tail: int = 10
    missing_diagrams: bool = True
    correlation_threshold: float = 0.9
    n_obs: Optional[int] = None


@dataclass
class CreateExplorationJobCommand:
    """Command to create an exploration job."""
    user_id: int
    dataset_id: int
    source_ref: str = "main"
    table_key: str = "primary"
    profile_config: Optional[ProfileConfig] = None


@dataclass
class GetExplorationHistoryCommand:
    """Command to get exploration history."""
    dataset_id: Optional[int] = None
    user_id: Optional[int] = None
    requesting_user_id: Optional[int] = None  # The user making the request
    offset: int = 0
    limit: int = 20


@dataclass
class GetExplorationResultCommand:
    """Command to get exploration result."""
    user_id: int
    job_id: UUID
    format: str = "html"  # html, json, info