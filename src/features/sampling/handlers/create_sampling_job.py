"""Handler for creating sampling jobs."""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from uuid import uuid4
import json
from ...base_handler import BaseHandler
from src.core.permissions import PermissionService
from src.core.domain_exceptions import EntityNotFoundException
from ..models import CreateSamplingJobCommand


@dataclass
class SamplingJobResponse:
    job_id: str
    status: str
    message: str


class CreateSamplingJobHandler(BaseHandler):
    """Handler for creating sampling jobs."""
    
    def __init__(self, uow: PostgresUnitOfWork, permissions: PermissionService):
        super().__init__(uow)
        self._permissions = permissions
    
    async def handle(self, command: CreateSamplingJobCommand) -> SamplingJobResponse:
        """
        Create a sampling job for asynchronous processing.
        
        Returns:
            SamplingJobResponse with job details
        """
        # Check permissions - write permission needed to create sampling job
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "write")
        
        async with self._uow:
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
            
            # Create job directly in the database
            job_id = await self._uow.jobs.create_job(
                run_type='sampling',
                dataset_id=command.dataset_id,
                source_commit_id=source_commit_id,
                user_id=command.user_id,
                run_parameters=job_params  # Pass as dict, not JSON string
            )
            
            await self._uow.commit()
            
            return SamplingJobResponse(
                job_id=str(job_id),
                status="pending",
                message=f"Sampling job created with {len(command.rounds)} rounds"
            )