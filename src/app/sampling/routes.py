from fastapi import APIRouter, Depends, Path, Body, HTTPException, status, Response
from typing import Dict, List, Any
from datetime import datetime

from app.sampling.models import SamplingRequest, SamplingJobResponse, SamplingJobDetails
from app.sampling.controller import SamplingController
from app.sampling.service import SamplingService
from app.datasets.repository import DatasetsRepository
from app.db.connection import get_session
from app.users.auth import get_current_user_info, CurrentUser
from sqlalchemy.ext.asyncio import AsyncSession

# Import the sampling repository
from app.sampling.repository import SamplingRepository

# Create a singleton instance of the sampling repository
# This ensures we have a single instance across all requests
sampling_repo = SamplingRepository()

# Create dependency for the controller
def get_sampling_controller(session: AsyncSession = Depends(get_session)):
    datasets_repository = DatasetsRepository(session)
    service = SamplingService(datasets_repository, sampling_repo)
    controller = SamplingController(service)
    return controller

# Create router
router = APIRouter(prefix="/api/sampling", tags=["Sampling"])

@router.post(
    "/{dataset_id}/{version_id}/run",
    response_model=SamplingJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a sampling job",
    description="""
    Create a new sampling job for a dataset version.
    
    The job will run asynchronously. A unique job ID is returned
    which can be used to poll for the job status and results.
    
    Different sampling methods require different parameters:
    
    - random: sample_size (required), seed (optional)
    - stratified: strata_columns (required), sample_size (optional), min_per_stratum (optional), seed (optional)
    - systematic: interval (required), start (optional)
    - cluster: cluster_column (required), num_clusters (required), sample_within_clusters (optional)
    - custom: query (required)
    """
)
async def create_sampling_job(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    request: SamplingRequest = Body(..., description="Sampling configuration"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Create a new sampling job"""
    # Use soeid from the CurrentUser object
    # In a real implementation, you might want to map this to a user ID in your database
    return await controller.create_sampling_job(
        dataset_id=dataset_id,
        version_id=version_id,
        request=request,
        user_id=current_user.role_id  # Using role_id as a simple user identifier for now
    )

@router.get(
    "/jobs/{run_id}",
    response_model=SamplingJobDetails,
    summary="Get sampling job details",
    description="""
    Get detailed information about a sampling job.
    
    This endpoint returns the current status of the job,
    along with preview data and output URI when available.
    """
)
async def get_job_details(
    run_id: str = Path(..., description="The ID of the sampling job"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Get details for a sampling job"""
    return await controller.get_job_details(run_id)

@router.get(
    "/jobs/{run_id}/preview",
    response_model=List[Dict[str, Any]],
    summary="Get sampling job preview",
    description="""
    Get a preview of the sampled data.
    
    This endpoint returns the first few rows of the sampled data
    once it's available. Returns an empty array if the preview
    is not yet ready.
    """
)
async def get_job_preview(
    run_id: str = Path(..., description="The ID of the sampling job"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Get preview data for a sampling job"""
    return await controller.get_job_preview(run_id)