"""Handler for creating sampling jobs."""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork
from src.core.services.sampling_service import SamplingJobManager
from ...base_handler import BaseHandler
from src.core.decorators import requires_permission
from src.core.domain_exceptions import EntityNotFoundException
from ..models import CreateSamplingJobCommand


@dataclass
class SamplingJobResponse:
    job_id: str
    status: str
    message: str


class CreateSamplingJobHandler(BaseHandler):
    """Handler for creating sampling jobs."""
    
    def __init__(self, uow: IUnitOfWork):
        super().__init__(uow)
    
    @requires_permission("datasets", "read")
    async def handle(self, command: CreateSamplingJobCommand) -> SamplingJobResponse:
        """
        Create a sampling job for asynchronous processing.
        
        Returns:
            SamplingJobResponse with job details
        """
        # Get current commit for ref
        ref = await self._uow.commits.get_ref(command.dataset_id, command.source_ref)
        if not ref:
            raise EntityNotFoundException("Ref", command.source_ref)
        
        source_commit_id = ref['commit_id']
        
        # Build job parameters
        job_params = {
            'source_commit_id': source_commit_id,
            'dataset_id': command.dataset_id,
            'table_key': command.table_key,
            'create_output_commit': True,  # Always create output commit
            'output_branch_name': command.output_branch_name,
            'commit_message': command.commit_message,
            'user_id': command.user_id,
            'rounds': command.rounds,
            'export_residual': command.export_residual,
            'residual_output_name': command.residual_output_name
        }
        
        # Create job using SamplingJobManager
        job_service = SamplingJobManager(self._uow)
        job_id = await job_service.create_sampling_job(
            dataset_id=command.dataset_id,
            source_commit_id=source_commit_id,
            user_id=command.user_id,
            sampling_config=job_params
        )
        
        return SamplingJobResponse(
            job_id=job_id,
            status="pending",
            message=f"Sampling job created with {len(command.rounds)} rounds"
        )