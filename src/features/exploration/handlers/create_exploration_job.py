"""Handler for creating exploration/profiling jobs."""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from uuid import UUID

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.domain_exceptions import EntityNotFoundException
from ..models import CreateExplorationJobCommand, ProfileConfig


@dataclass
class ExplorationJobResponse:
    """Response for exploration job creation."""
    job_id: str
    status: str
    message: str


class CreateExplorationJobHandler:
    """Handler for creating exploration jobs."""
    
    def __init__(self, uow: PostgresUnitOfWork):
        self._uow = uow
    
    async def handle(self, command: CreateExplorationJobCommand) -> ExplorationJobResponse:
        """Create an exploration job."""
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