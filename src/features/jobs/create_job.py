"""Handler for creating jobs."""

from typing import Dict, Any, Optional
from dataclasses import dataclass
from uuid import UUID
from datetime import datetime
from src.core.abstractions import IUnitOfWork, IJobRepository
from src.features.base_handler import BaseHandler, with_transaction
from src.core.decorators import requires_permission


@dataclass
class CreateJobCommand:
    user_id: int  # Must be first for decorator
    dataset_id: int
    run_type: str
    source_commit_id: str
    run_parameters: Dict[str, Any]
    description: Optional[str] = None


@dataclass
class CreateJobResponse:
    job_id: UUID
    status: str
    created_at: datetime


class CreateJobHandler(BaseHandler):
    """Handler for creating new jobs."""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        job_repo: IJobRepository
    ):
        super().__init__(uow)
        self._job_repo = job_repo
    
    @with_transaction
    @requires_permission("datasets", "write")  # Need write permission to create jobs
    async def handle(self, command: CreateJobCommand) -> CreateJobResponse:
        """
        Create a new job for processing.
        
        Valid run_types: import, sampling, exploration, sql_transform
        """
        # Validate run type
        valid_run_types = ['import', 'sampling', 'exploration', 'sql_transform']
        if command.run_type not in valid_run_types:
            raise ValueError(f"Invalid run_type. Must be one of: {valid_run_types}")
        
        # Create job
        job_id = await self._job_repo.create_job(
            run_type=command.run_type,
            user_id=command.user_id,
            dataset_id=command.dataset_id,
            source_commit_id=command.source_commit_id,
            run_parameters=command.run_parameters,
            description=command.description
        )
        
        return CreateJobResponse(
            job_id=job_id,
            status='pending',
            created_at=datetime.utcnow()
        )