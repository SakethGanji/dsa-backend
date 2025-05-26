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
    
    ## Sampling Methods
    
    Different sampling methods require different parameters:
    
    - **random**: sample_size (required), seed (optional)
    - **stratified**: strata_columns (required), sample_size (optional), min_per_stratum (optional), seed (optional)
    - **systematic**: interval (required), start (optional)
    - **cluster**: cluster_column (required), num_clusters (required), sample_within_clusters (optional)
    - **custom**: query (required)
    
    ## Filtering and Selection
    
    You can also specify:
    
    - **filters**: Row filtering with conditions (column, operator, value)
      - Operators: =, !=, >, <, >=, <=, LIKE, ILIKE, IN, NOT IN, IS NULL, IS NOT NULL
      - Logic: AND/OR between conditions
    - **selection**: Column selection and data ordering
      - columns: List of columns to include (null = all)
      - exclude_columns: List of columns to exclude
      - order_by: Column to sort by
      - order_desc: Use descending order
      - limit/offset: Pagination before sampling
    
    ## Example Request
    
    ```json
    {
      "method": "random",
      "parameters": {"sample_size": 1000},
      "output_name": "my_sample",
      "filters": {
        "conditions": [
          {"column": "age", "operator": ">", "value": 18},
          {"column": "status", "operator": "IN", "value": ["active", "pending"]}
        ],
        "logic": "AND"
      },
      "selection": {
        "columns": ["name", "age", "status"],
        "order_by": "age",
        "order_desc": false
      }
    }
    ```
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

@router.get(
    "/{dataset_id}/{version_id}/columns",
    response_model=Dict[str, Any],
    summary="Get dataset column information",
    description="""
    Get column information for a dataset version.
    
    This endpoint returns column names, types, and basic statistics
    which can be used to build filters and understand the data structure.
    """
)
async def get_dataset_columns(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Get column information for a dataset version"""
    return await controller.get_dataset_columns(dataset_id, version_id)

@router.post(
    "/{dataset_id}/{version_id}/execute",
    response_model=List[Dict[str, Any]],
    status_code=status.HTTP_200_OK,
    summary="Execute sampling synchronously and return data",
    description="""
    Execute a sampling request synchronously and return the sampled data directly.
    
    This endpoint is suitable for smaller datasets or quick sampling operations.
    For larger datasets or long-running operations, using the asynchronous `/run` 
    endpoint is recommended to avoid request timeouts.
    
    Refer to the `/run` endpoint for details on Sampling Methods, Filtering, and Selection.
    The request body structure is identical.
    """
)
async def execute_sampling_sync(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    request: SamplingRequest = Body(..., description="Sampling configuration"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Execute sampling synchronously and return data directly"""
    return await controller.execute_sampling_sync(
        dataset_id=dataset_id,
        version_id=version_id,
        request=request,
        user_id=current_user.role_id
    )

