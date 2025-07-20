from uuid import UUID

from src.core.abstractions import IJobRepository
from src.api.models import JobStatusResponse
from src.core.domain_exceptions import EntityNotFoundException


class GetJobStatusHandler:
    """Handler for retrieving job status"""
    
    def __init__(self, job_repo: IJobRepository):
        self._job_repo = job_repo
    
    async def handle(self, job_id: UUID, user_id: int) -> JobStatusResponse:
        """
        Get status of a job
        
        Steps:
        1. Fetch job from database
        2. Verify user owns the job
        3. Return status information
        """
        # Get job details
        job = await self._job_repo.get_job_by_id(job_id)
        if not job:
            raise EntityNotFoundException("Job", job_id)
        
        # Verify user owns the job or has permission
        if job['user_id'] != user_id:
            # In production, might want to check if user has admin permission
            raise ForbiddenException()
        
        return JobStatusResponse(
            job_id=job['id'],
            run_type=job['run_type'],
            status=job['status'],
            dataset_id=job['dataset_id'],
            created_at=job['created_at'],
            completed_at=job.get('completed_at'),
            error_message=job.get('error_message'),
            output_summary=job.get('output_summary')
        )