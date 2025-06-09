from fastapi import APIRouter, Depends, Path, Body, HTTPException, status, Response, Query
from typing import Dict, List, Any
from datetime import datetime

from app.sampling.models import (
    SamplingRequest, SamplingJobResponse, SamplingJobDetails,
    MultiRoundSamplingRequest, MultiRoundSamplingJobResponse
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
    service = SamplingService(datasets_repository, sampling_repo, storage_backend)
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
    response_model=Dict[str, Any],
    summary="Get sampling job preview",
    description="""
    Get a preview of the sampled data.
    
    This endpoint returns the first few rows of the sampled data
    once it's available. Returns an empty array if the preview
    is not yet ready.
    
    Supports pagination with page and page_size parameters.
    """
)
async def get_job_preview(
    run_id: str = Path(..., description="The ID of the sampling job"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=1, le=1000, description="Number of items per page"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Get preview data for a sampling job"""
    return await controller.get_job_preview(run_id, page=page, page_size=page_size)

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
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
    summary="Execute sampling synchronously and return data",
    description="""
    Execute a sampling request synchronously and return the sampled data directly.
    
    This endpoint is suitable for smaller datasets or quick sampling operations.
    For larger datasets or long-running operations, using the asynchronous `/run` 
    endpoint is recommended to avoid request timeouts.
    
    Supports pagination with page and page_size parameters.
    
    Refer to the `/run` endpoint for details on Sampling Methods, Filtering, and Selection.
    The request body structure is identical.
    """
)
async def execute_sampling_sync(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    request: SamplingRequest = Body(..., description="Sampling configuration"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=1, le=1000, description="Number of items per page"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Execute sampling synchronously and return data directly"""
    return await controller.execute_sampling_sync(
        dataset_id=dataset_id,
        version_id=version_id,
        request=request,
        user_id=current_user.role_id,
        page=page,
        page_size=page_size
    )

# Multi-round sampling endpoints
@router.post(
    "/{dataset_id}/{version_id}/multi-round/run",
    response_model=MultiRoundSamplingJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a multi-round sampling job",
    description="""
    Create a new multi-round sampling job for progressive residual sampling.
    
    This allows you to perform multiple rounds of sampling where each round
    samples from the remaining data after previous rounds.
    
    ## Key Features:
    
    - **Progressive Sampling**: Each round samples from the residual dataset
    - **Multiple Methods**: Each round can use a different sampling method
    - **Flexible Configuration**: Configure sample size, method, and filters per round
    - **Residual Export**: Option to export the final un-sampled residual dataset
    
    ## Example Request:
    
    ```json
    {
        "rounds": [
            {
                "round_number": 1,
                "method": "random",
                "parameters": {"sample_size": 1000, "seed": 42},
                "output_name": "round_1_random"
            },
            {
                "round_number": 2,
                "method": "stratified",
                "parameters": {
                    "strata_columns": ["category"],
                    "sample_size": 500
                },
                "output_name": "round_2_stratified"
            }
        ],
        "export_residual": true,
        "residual_output_name": "final_residual"
    }
    ```
    """
)
async def create_multi_round_sampling_job(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    request: MultiRoundSamplingRequest = Body(..., description="Multi-round sampling configuration"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Create a multi-round sampling job for progressive residual sampling"""
    return await controller.create_multi_round_sampling_job(
        dataset_id=dataset_id,
        version_id=version_id,
        request=request,
        user_id=current_user.role_id
    )

@router.get(
    "/multi-round/jobs/{job_id}",
    response_model=MultiRoundSamplingJobResponse,
    summary="Get multi-round sampling job status",
    description="""
    Get the status and results of a multi-round sampling job.
    
    Returns:
    - Overall job status and progress
    - Results from completed rounds
    - Residual dataset information
    - Error information if job failed
    """
)
async def get_multi_round_job(
    job_id: str = Path(..., description="The multi-round job ID"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Get status and results of a multi-round sampling job"""
    return await controller.get_multi_round_job(job_id)

@router.get(
    "/multi-round/jobs/{job_id}/round/{round_number}/preview",
    response_model=Dict[str, Any],
    summary="Get preview of a specific round's sample",
    description="""
    Get a preview of the data sampled in a specific round.
    
    Returns paginated preview data from the specified round.
    """
)
async def get_round_preview(
    job_id: str = Path(..., description="The multi-round job ID"),
    round_number: int = Path(..., description="The round number"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=1, le=1000, description="Number of items per page"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Get preview data from a specific sampling round"""
    return await controller.get_round_preview(
        job_id=job_id,
        round_number=round_number,
        page=page,
        page_size=page_size
    )

@router.get(
    "/multi-round/jobs/{job_id}/residual/preview",
    response_model=Dict[str, Any],
    summary="Get preview of residual dataset",
    description="""
    Get a preview of the final residual dataset after all sampling rounds.
    
    Returns paginated preview data from the residual dataset.
    Only available if export_residual was set to true.
    """
)
async def get_residual_preview(
    job_id: str = Path(..., description="The multi-round job ID"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=1, le=1000, description="Number of items per page"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Get preview data from the residual dataset"""
    return await controller.get_residual_preview(
        job_id=job_id,
        page=page,
        page_size=page_size
    )

@router.post(
    "/{dataset_id}/{version_id}/multi-round/execute",
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
    summary="Execute multi-round sampling synchronously",
    description="""
    Execute multi-round sampling synchronously and return all results directly.
    
    This endpoint performs progressive residual sampling across multiple rounds
    and returns the data immediately. Each round samples from the remaining data
    after previous rounds.
    
    ## Key Features:
    
    - **Synchronous Execution**: All rounds are processed immediately
    - **Direct Data Return**: Returns sampled data from all rounds
    - **Progressive Sampling**: Each round samples from the residual dataset
    - **Flexible Configuration**: Configure sample size, method, and filters per round
    - **Residual Export**: Option to include the final un-sampled residual dataset
    
    ## Example Request:
    
    ```json
    {
        "rounds": [
            {
                "round_number": 1,
                "method": "random",
                "parameters": {"sample_size": 1000, "seed": 42},
                "output_name": "round_1_random"
            },
            {
                "round_number": 2,
                "method": "stratified",
                "parameters": {
                    "strata_columns": ["category"],
                    "sample_size": 500
                },
                "output_name": "round_2_stratified"
            }
        ],
        "export_residual": true,
        "residual_output_name": "final_residual"
    }
    ```
    
    ## Response Structure:
    
    The response includes:
    - **rounds**: Array of round results with data, sample size, and summary
    - **residual**: Final residual dataset (if export_residual is true)
    
    Supports pagination with page and page_size parameters.
    """
)
async def execute_multi_round_sampling_sync(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    request: MultiRoundSamplingRequest = Body(..., description="Multi-round sampling configuration"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=1, le=1000, description="Number of items per page"),
    controller: SamplingController = Depends(get_sampling_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Execute multi-round sampling synchronously and return data directly"""
    return await controller.execute_multi_round_sampling_sync(
        dataset_id=dataset_id,
        version_id=version_id,
        request=request,
        user_id=current_user.role_id,
        page=page,
        page_size=page_size
    )

