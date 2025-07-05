"""Routes for sampling API v2 - Git-like versioning system"""
from fastapi import APIRouter, Depends, Path, Body, HTTPException, status, Response, Query
from typing import Dict, List, Any, Optional
from datetime import datetime
import uuid

from app.sampling.models import (
    SamplingRequest,
    MultiRoundSamplingRequest, MultiRoundSamplingJobResponse,
    AnalysisRunListResponse
)
from app.sampling.controller import SamplingController
from app.sampling.service import SamplingService
from app.datasets.repository import DatasetsRepository
from app.db.connection import get_session
from app.users.auth import get_current_user_info, CurrentUser
from sqlalchemy.ext.asyncio import AsyncSession

# Import the sampling repository
from app.sampling.repository import SamplingRepository
from app.storage.factory import StorageFactory

# Create a singleton instance of the sampling repository
# This ensures we have a single instance across all requests
sampling_repo = SamplingRepository()

# Create dependency for the controller
def get_sampling_controller(session: AsyncSession = Depends(get_session)):
    datasets_repository = DatasetsRepository(session)
    storage_backend = StorageFactory.get_instance()
    # Pass the database session to enable database persistence
    service = SamplingService(datasets_repository, sampling_repo, storage_backend, db_session=session)
    controller = SamplingController(service)
    return controller

# Create router - no /api prefix in v2
router = APIRouter(tags=["Sampling"])


@router.post(
    "/datasets/{dataset_id}/commits/{commit_hash}/samples",
    response_model=Dict[str, Any],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create sampling job",
    description="""
    Create a new sampling job from a specific commit.
    
    This is an asynchronous operation that:
    1. Creates a job in the analysis_runs table
    2. Executes sampling based on the provided configuration
    3. Creates a new commit with the sampled data
    4. Creates a new ref (branch) pointing to the sample commit
    
    The job status can be tracked via the /jobs/{job_id} endpoint.
    """
)
async def create_sampling_job(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    commit_hash: str = Path(..., regex="^[a-f0-9]{64}$", description="Source commit hash"),
    request: MultiRoundSamplingRequest = Body(..., description="Sampling configuration"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
) -> Dict[str, Any]:
    """Create a sampling job that produces a new commit and ref."""
    # This will:
    # 1. Create job in analysis_runs with run_type='sampling'
    # 2. Return job_id for tracking
    # 3. Background worker executes sampling
    # 4. Creates new commit with sampled rows
    # 5. Creates ref like "samples/job-{job_id}"
    raise NotImplementedError("Create sampling job endpoint")


@router.get(
    "/datasets/{dataset_id}/commits/{commit_hash}/schema",
    response_model=Dict[str, Any],
    summary="Get dataset schema for sampling",
    description="""
    Get column information and schema for a dataset commit.
    
    This endpoint returns column names, types, and basic statistics
    which can be used to build filters and understand the data structure
    before sampling.
    """
)
async def get_commit_schema_for_sampling(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    commit_hash: str = Path(..., regex="^[a-f0-9]{64}$", description="Commit hash"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
) -> Dict[str, Any]:
    """Get schema information for sampling from a commit."""
    # Returns schema from commit_schemas table
    raise NotImplementedError("Get commit schema for sampling endpoint")


# Legacy endpoints mapped to new system - these will be deprecated
@router.get(
    "/sampling/user/{user_id}/samplings",
    response_model=AnalysisRunListResponse,
    summary="Get all samplings by user ID (DEPRECATED)",
    description="""
    DEPRECATED: Use /jobs?user_id={user_id}&run_type=sampling instead.
    
    Retrieve all sampling runs created by a specific user.
    """
)
async def get_samplings_by_user(
    user_id: int = Path(..., description="The ID of the user"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(10, ge=1, le=100, description="Number of items per page"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
) -> AnalysisRunListResponse:
    """Get all sampling runs created by a specific user."""
    # This should query analysis_runs table with filters:
    # - user_id = user_id
    # - run_type = 'sampling'
    raise NotImplementedError("Query sampling jobs by user")


@router.get(
    "/sampling/dataset/{dataset_id}/samplings",
    response_model=AnalysisRunListResponse,
    summary="Get all samplings by dataset ID (DEPRECATED)",
    description="""
    DEPRECATED: Use /datasets/{dataset_id}/jobs?run_type=sampling instead.
    
    Retrieve all sampling runs performed on a specific dataset across all commits.
    """
)
async def get_samplings_by_dataset(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(10, ge=1, le=100, description="Number of items per page"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
) -> AnalysisRunListResponse:
    """Get all sampling runs for a specific dataset."""
    # This should query analysis_runs table with filters:
    # - dataset_id = dataset_id
    # - run_type = 'sampling'
    raise NotImplementedError("Query sampling jobs by dataset")