"""Consolidated service for all exploration operations."""

from typing import Dict, Any, List, Optional
from uuid import UUID
from dataclasses import dataclass
import json

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.domain_exceptions import EntityNotFoundException, ValidationException, BusinessRuleViolation
from ...base_handler import with_transaction, with_error_handling
from ..models import (
    CreateExplorationJobCommand,
    GetExplorationHistoryCommand,
    GetExplorationResultCommand
)


@dataclass
class ExplorationJobResponse:
    """Response for exploration job creation."""
    job_id: str
    status: str
    message: str


@dataclass 
class ExplorationHistoryItem:
    """Single item in exploration history."""
    job_id: str
    dataset_id: int
    dataset_name: str
    user_id: int
    username: str
    status: str
    created_at: str
    updated_at: Optional[str]
    run_parameters: Dict[str, Any]
    has_result: bool


@dataclass
class ExplorationHistoryResponse:
    """Response for exploration history."""
    items: List[ExplorationHistoryItem]
    total: int
    offset: int
    limit: int


class ExplorationService:
    """Consolidated service for all exploration operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork
    ):
        self._uow = uow
    
    @with_transaction
    @with_error_handling
    async def create_exploration_job(
        self,
        command: CreateExplorationJobCommand
    ) -> ExplorationJobResponse:
        """Create an exploration job for asynchronous processing."""
        # Get current commit for ref
        ref = await self._uow.commits.get_ref(command.dataset_id, command.source_ref)
        if not ref:
            raise EntityNotFoundException("Ref", command.source_ref)
        
        source_commit_id = ref['commit_id']
        
        # Convert profile config to dict
        profile_config_dict = None
        if command.profile_config:
            profile_config_dict = {
                "minimal": command.profile_config.minimal,
                "samples": {
                    "head": command.profile_config.samples_head,
                    "tail": command.profile_config.samples_tail
                },
                "missing_diagrams": {
                    "bar": command.profile_config.missing_diagrams,
                    "matrix": command.profile_config.missing_diagrams
                },
                "correlations": {
                    "pearson": {
                        "calculate": True,
                        "threshold": command.profile_config.correlation_threshold
                    }
                }
            }
            
            if command.profile_config.n_obs:
                profile_config_dict["n_obs"] = command.profile_config.n_obs
        
        # Create exploration job
        job_params = {
            "table_key": command.table_key,
            "profile_config": profile_config_dict or {},
            "output_format": "html"
        }
        
        job_id = await self._uow.jobs.create_job(
            run_type="exploration",
            dataset_id=command.dataset_id,
            user_id=command.user_id,
            source_commit_id=source_commit_id,
            run_parameters=job_params
        )
        
        await self._uow.commit()
        
        return ExplorationJobResponse(
            job_id=str(job_id),
            status="pending",
            message="Exploration job created successfully"
        )
    
    @with_error_handling
    async def get_exploration_history(
        self,
        dataset_id: int,
        user_id: int,
        limit: int = 10,
        offset: int = 0,
        status: Optional[str] = None
    ) -> ExplorationHistoryResponse:
        """Get exploration history for a dataset."""
        async with self._uow:
            # Get exploration history
            history_items = await self._uow.explorations.get_exploration_history(
                dataset_id=dataset_id,
                user_id=user_id,
                limit=limit,
                offset=offset
            )
            
            # Convert to response items
            items = [
                ExplorationHistoryItem(
                    job_id=item["job_id"],
                    dataset_id=item["dataset_id"],
                    dataset_name=item["dataset_name"],
                    user_id=item["user_id"],
                    username=item["username"],
                    status=item["status"],
                    created_at=item["created_at"],
                    updated_at=item["updated_at"],
                    run_parameters=item["run_parameters"],
                    has_result=item["has_result"]
                )
                for item in history_items
            ]
            
            # Get total count
            total = await self._uow.explorations.count_explorations(
                dataset_id=dataset_id,
                user_id=user_id
            )
            
            return ExplorationHistoryResponse(
                items=items,
                total=total,
                offset=offset,
                limit=limit
            )
    
    @with_error_handling
    async def get_exploration_result(
        self,
        job_id: UUID,
        user_id: int,
        format: str = "html"
    ) -> Dict[str, Any]:
        """Get results from a completed exploration job."""
        # Validate format
        if format not in ["html", "json", "info"]:
            raise ValidationException(f"Invalid format: {format}", field="format")
        
        async with self._uow:
            # Get job details
            job = await self._uow.jobs.get_job_by_id(job_id)
            if not job:
                raise EntityNotFoundException("Job", job_id)
            
            if job["run_type"] != "exploration":
                raise ValidationException("Not an exploration job", field="run_type")
            
            if job["status"] != "completed":
                raise BusinessRuleViolation(
                    f"Job is {job['status']}, not completed", 
                    rule="job_must_be_completed"
                )
            
            # Get exploration result
            output_summary = await self._uow.explorations.get_exploration_result(job_id)
            
            if not output_summary:
                raise EntityNotFoundException("Result", job_id)
            
            # Return appropriate response based on format
            if format == "html":
                return {
                    "content": output_summary.get("profile_html", ""),
                    "content_type": "text/html"
                }
            elif format == "json":
                return {
                    "content": json.loads(output_summary.get("profile_json", "{}")),
                    "content_type": "application/json"
                }
            else:  # info
                return {
                    "content": output_summary.get("dataset_info", {}),
                    "content_type": "application/json"
                }