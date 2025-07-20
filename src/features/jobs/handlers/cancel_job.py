"""Handler for cancelling jobs."""

from dataclasses import dataclass
from uuid import UUID
from typing import Optional
from src.core.abstractions import IUnitOfWork, IJobRepository
from src.core.abstractions.events import IEventBus, JobCancelledEvent
from ...base_handler import BaseHandler, with_transaction
from src.core.decorators import requires_permission
from src.core.domain_exceptions import EntityNotFoundException, ForbiddenException, BusinessRuleViolation
from ..models import CancelJobCommand, Job, JobParameters, JobType, JobStatus


@dataclass
class CancelJobResponse:
    job_id: UUID
    status: str
    message: str


class CancelJobHandler(BaseHandler):
    """Handler for cancelling running or pending jobs."""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        job_repo: IJobRepository,
        event_bus: Optional[IEventBus] = None
    ):
        super().__init__(uow)
        self._job_repo = job_repo
        self._event_bus = event_bus
    
    @with_transaction
    @requires_permission("datasets", "write")  # Need write permission to cancel jobs
    async def handle(self, command: CancelJobCommand) -> CancelJobResponse:
        """
        Cancel a job if it's still pending or running.
        
        Only the job creator or dataset admin can cancel jobs.
        """
        # Get job details
        job_data = await self._job_repo.get_job_by_id(command.job_id)
        if not job_data:
            raise EntityNotFoundException("Job", command.job_id)
        
        # Verify job belongs to the dataset
        if job_data['dataset_id'] != command.dataset_id:
            raise ValueError("Job does not belong to the specified dataset")
        
        # Create domain model from repository data
        job_params = JobParameters(
            dataset_id=job_data['dataset_id'],
            user_id=job_data['user_id'],
            job_type=JobType.from_string(job_data['run_type']),
            parameters=job_data.get('run_parameters', {})
        )
        
        job = Job(
            id=job_data['job_id'],
            parameters=job_params,
            status=JobStatus(job_data['status']),
            created_at=job_data['created_at'],
            started_at=job_data.get('started_at'),
            completed_at=job_data.get('completed_at'),
            source_commit_id=job_data.get('source_commit_id'),
            output_commit_id=job_data.get('output_commit_id'),
            progress_percentage=job_data.get('progress_percentage', 0)
        )
        
        # Additional check: only job creator or admin can cancel
        user = await self._uow.users.get_by_id(command.user_id)
        is_admin = user and user.get('role_name') == 'admin'
        
        if not is_admin and job.parameters.user_id != command.user_id:
            raise ForbiddenException("Only job creator or admin can cancel this job")
        
        # Use domain method to cancel the job
        try:
            job.cancel(command.user_id)
        except BusinessRuleViolation as e:
            raise ValueError(str(e))
        
        # Persist the cancellation
        await self._job_repo.update_job_status(
            job_id=job.id,
            status=job.status.value,
            error_message=job.result.error_message if job.result else "Job cancelled by user"
        )
        
        # Publish event
        if self._event_bus:
            await self._event_bus.publish(JobCancelledEvent(
                job_id=str(job.id),
                job_type=job.parameters.job_type.value,
                cancelled_by=command.user_id,
                reason="User requested cancellation"
            ))
        
        return CancelJobResponse(
            job_id=command.job_id,
            status='cancelled',
            message="Job has been cancelled successfully"
        )