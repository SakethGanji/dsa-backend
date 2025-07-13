"""Handler for cancelling jobs."""

from dataclasses import dataclass
from uuid import UUID
from src.core.abstractions import IUnitOfWork, IJobRepository
from src.features.base_handler import BaseHandler, with_transaction
from src.core.decorators import requires_permission
from src.core.domain_exceptions import EntityNotFoundException


@dataclass
class CancelJobCommand:
    user_id: int  # Must be first for decorator
    job_id: UUID
    dataset_id: int  # For permission check


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
        job_repo: IJobRepository
    ):
        super().__init__(uow)
        self._job_repo = job_repo
    
    @with_transaction
    @requires_permission("datasets", "write")  # Need write permission to cancel jobs
    async def handle(self, command: CancelJobCommand) -> CancelJobResponse:
        """
        Cancel a job if it's still pending or running.
        
        Only the job creator or dataset admin can cancel jobs.
        """
        # Get job details
        job = await self._job_repo.get_job_by_id(command.job_id)
        if not job:
            raise EntityNotFoundException("Job", command.job_id)
        
        # Verify job belongs to the dataset
        if job['dataset_id'] != command.dataset_id:
            raise ValueError("Job does not belong to the specified dataset")
        
        # Check if job can be cancelled
        if job['status'] not in ['pending', 'running']:
            raise ValueError(f"Cannot cancel job with status: {job['status']}")
        
        # Additional check: only job creator or admin can cancel
        user = await self._uow.users.get_by_id(command.user_id)
        is_admin = user and user.get('role_name') == 'admin'
        
        if not is_admin and job['user_id'] != command.user_id:
            raise ForbiddenException()
        
        # Cancel the job
        await self._job_repo.update_job_status(
            job_id=command.job_id,
            status='cancelled',
            error_message="Job cancelled by user"
        )
        
        return CancelJobResponse(
            job_id=command.job_id,
            status='cancelled',
            message="Job has been cancelled successfully"
        )