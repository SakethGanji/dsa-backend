from fastapi import APIRouter, Depends
from uuid import UUID

from models.pydantic_models import JobStatusResponse
from features.jobs.get_job_status import GetJobStatusHandler

# Dependency injection
async def get_current_user_id() -> int:
    # TODO: Extract from JWT token
    return 1

async def get_job_status_handler() -> GetJobStatusHandler:
    # TODO: Wire up dependencies
    pass


router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job_status(
    job_id: UUID,
    user_id: int = Depends(get_current_user_id),
    handler: GetJobStatusHandler = Depends(get_job_status_handler)
):
    """Get status of a job"""
    return await handler.handle(job_id, user_id)