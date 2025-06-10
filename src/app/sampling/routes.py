from fastapi import APIRouter, Depends, Path, Body, HTTPException, status, Response, Query
from typing import Dict, List, Any
from datetime import datetime

from app.sampling.models import (
    SamplingRequest,
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

