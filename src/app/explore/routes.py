"""Routes for explore API v2 - Git-like versioning system"""
from fastapi import APIRouter, Depends, Path, Body, HTTPException, status, Query
from fastapi.responses import StreamingResponse
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

from app.explore.models import ExploreRequest
from app.explore.controller import ExploreController
from app.explore.service import ExploreService
from app.datasets.repository import DatasetsRepository
from app.db.connection import get_session
from app.users.models import UserOut as User
from sqlalchemy.ext.asyncio import AsyncSession

# Import the auth dependencies from users module
from app.users.auth import get_current_user_info, CurrentUser
from app.storage.factory import StorageFactory

# Create dependency for the controller
def get_explore_controller(session: AsyncSession = Depends(get_session)):
    repository = DatasetsRepository(session)
    storage_backend = StorageFactory.get_instance()
    service = ExploreService(repository, storage_backend)
    controller = ExploreController(service)
    return controller


# Create router - no /api prefix in v2
router = APIRouter(tags=["Explore"])


@router.post(
    "/datasets/{dataset_id}/commits/{commit_hash}/explorations",
    response_model=Dict[str, Any],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create exploration job",
    description="""
    Create an async job to explore/profile a dataset commit.
    
    This endpoint creates an exploration job that:
    1. Loads the dataset from the specified commit
    2. Generates profiling statistics and summaries
    3. Stores results in the output_summary field of analysis_runs
    
    The job status can be tracked via the /jobs/{job_id} endpoint.
    """
)
async def create_exploration_job(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    commit_hash: str = Path(..., regex="^[a-f0-9]{64}$", description="Commit hash to explore"),
    request: ExploreRequest = Body(..., description="Options for dataset exploration"),
    controller: ExploreController = Depends(get_explore_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
) -> Dict[str, Any]:
    """Create an exploration job for a dataset commit."""
    # This will:
    # 1. Create job in analysis_runs with run_type='exploration'
    # 2. Return job_id for tracking
    # 3. Background worker loads data from commit and generates profile
    # 4. Results stored in output_summary as JSON
    raise NotImplementedError("Create exploration job endpoint")


# Legacy endpoint - will be deprecated
@router.post(
    "/explore/{dataset_id}/{version_id}",
    response_model=Dict[str, Any],
    summary="Explore a dataset version (DEPRECATED)",
    description="""
    DEPRECATED: Use POST /datasets/{dataset_id}/commits/{commit_hash}/explorations instead.
    
    This endpoint is maintained for backwards compatibility but will be removed in a future version.
    """
)
async def explore_dataset_legacy(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    request: ExploreRequest = Body(..., description="Options for dataset exploration"),
    controller: ExploreController = Depends(get_explore_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
) -> Dict[str, Any]:
    """Legacy explore endpoint - maps version to commit."""
    # This would need to:
    # 1. Look up the commit hash for this version_id
    # 2. Call the new exploration job endpoint
    raise NotImplementedError("Legacy explore endpoint")