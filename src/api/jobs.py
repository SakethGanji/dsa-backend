from fastapi import APIRouter, Depends, Query, status
from uuid import UUID
from typing import Optional, List

from src.api.models import (
    JobListResponse, JobDetailResponse, JobSummary, JobDetail
)
from src.features.jobs.handlers.get_jobs import GetJobsHandler
from src.features.jobs.handlers.get_job_by_id import GetJobByIdHandler
from src.core.authorization import get_current_user_info
from src.core.domain_exceptions import resource_not_found
from src.api.dependencies import get_uow
from src.core.abstractions import IUnitOfWork
from src.api.models import CurrentUser


router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("", response_model=JobListResponse)
async def get_jobs(
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    user_soeid: Optional[str] = Query(None, description="Filter by user SOEID"),
    dataset_id: Optional[int] = Query(None, description="Filter by dataset ID"),
    status: Optional[str] = Query(None, description="Filter by status"),
    run_type: Optional[str] = Query(None, description="Filter by run type"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=1000, description="Pagination limit"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: IUnitOfWork = Depends(get_uow)
):
    """Get list of jobs with optional filters"""
    handler = GetJobsHandler(uow)
    
    # Convert SOEID to user_id if provided
    filter_user_id = user_id
    if user_soeid and not user_id:
        # Look up user by SOEID
        user = await uow.connection.fetchrow(
            "SELECT id FROM dsa_auth.users WHERE soeid = $1",
            user_soeid
        )
        if user:
            filter_user_id = user['id']
    
    result = await handler.handle(
        user_id=filter_user_id,
        dataset_id=dataset_id,
        status=status,
        run_type=run_type,
        offset=offset,
        limit=limit,
        current_user_id=current_user.user_id
    )
    
    # Convert to response model
    jobs = [
        JobSummary(
            job_id=job["id"],
            run_type=job["run_type"],
            status=job["status"],
            dataset_id=job["dataset_id"],
            dataset_name=job["dataset_name"],
            user_id=job["user_id"],
            user_soeid=job["user_soeid"],
            created_at=job["created_at"],
            updated_at=job["updated_at"],
            completed_at=job["completed_at"],
            error_message=job["error_message"]
        )
        for job in result["jobs"]
    ]
    
    return JobListResponse(
        jobs=jobs,
        total=result["total"],
        offset=result["offset"],
        limit=result["limit"]
    )


@router.get("/{job_id}", response_model=JobDetail)
async def get_job_by_id(
    job_id: UUID,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: IUnitOfWork = Depends(get_uow)
):
    """Get detailed information about a specific job"""
    handler = GetJobByIdHandler(uow)
    
    job = await handler.handle(
        job_id=job_id,
        current_user_id=current_user.user_id
    )
    
    if not job:
        raise resource_not_found("Job", job_id)
    
    # Permission check is now handled by the handler using decorators
    
    return JobDetail(
        job_id=job["id"],
        run_type=job["run_type"],
        status=job["status"],
        dataset_id=job["dataset_id"],
        dataset_name=job["dataset_name"],
        source_commit_id=job["source_commit_id"],
        user_id=job["user_id"],
        user_soeid=job["user_soeid"],
        run_parameters=job["run_parameters"],
        output_summary=job["output_summary"],
        error_message=job["error_message"],
        created_at=job["created_at"],
        updated_at=job.get("updated_at", job["created_at"]),  # Use created_at if updated_at not available
        completed_at=job["completed_at"]
    )