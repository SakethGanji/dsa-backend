"""Handler for creating jobs."""

from typing import Dict, Any, Optional
from dataclasses import dataclass
from uuid import UUID
from datetime import datetime
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.job_repo import PostgresJobRepository
from src.core.events.publisher import EventBus, DomainEvent
from ...base_handler import BaseHandler, with_transaction
from src.core.permissions import PermissionService
from ..models import CreateJobCommand


@dataclass
class JobCreatedEvent(DomainEvent):
    """Event raised when a job is created."""
    job_id: UUID
    run_type: str
    user_id: int
    dataset_id: int
    
    def __post_init__(self):
        super().__init__()


@dataclass
class CreateJobResponse:
    job_id: UUID
    status: str
    created_at: datetime


class CreateJobHandler(BaseHandler):
    """Handler for creating new jobs."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        job_repo: PostgresJobRepository,
        permissions: PermissionService,
        event_bus: Optional[EventBus] = None
    ):
        super().__init__(uow)
        self._job_repo = job_repo
        self._permissions = permissions
        self._event_bus = event_bus
    
    @with_transaction
    async def handle(self, command: CreateJobCommand) -> CreateJobResponse:
        """
        Create a new job for processing.
        
        Valid run_types: import, sampling, exploration, sql_transform
        """
        # Check write permission on the dataset
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "write")
        
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
        
        # Publish event
        if self._event_bus:
            await self._event_bus.publish(JobCreatedEvent(
                job_id=str(job_id),
                job_type=command.run_type,
                dataset_id=command.dataset_id,
                created_by=command.user_id
            ))
        
        return CreateJobResponse(
            job_id=job_id,
            status='pending',
            created_at=datetime.utcnow()
        )