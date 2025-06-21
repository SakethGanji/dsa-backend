from fastapi import APIRouter, Depends, Path, Body, HTTPException, status, Query
from fastapi.responses import StreamingResponse
from typing import Dict, Any, Optional
from datetime import datetime

from app.explore.models import ExploreRequest
from app.explore.eda_models import EDARequest, EDAResponse, AnalysisConfig
from app.explore.controller import ExploreController
from app.explore.service import ExploreService
from app.explore.eda_service import EDAService
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

# Create dependency for EDA service
def get_eda_service(session: AsyncSession = Depends(get_session)):
    repository = DatasetsRepository(session)
    storage_backend = StorageFactory.get_instance()
    return EDAService(session, repository, storage_backend)

# Create router
router = APIRouter(prefix="/api/explore", tags=["Explore"])

@router.post(
    "/{dataset_id}/{version_id}",
    response_model=Dict[str, Any],
    summary="Explore a dataset version",
    description="""
    Load a dataset and generate a profile report or summary.

    Steps:
    1. Load the dataset
    2. Generate a summary or full profile report
    """
)
async def explore_dataset(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    request: ExploreRequest = Body(..., description="Options for dataset exploration"),
    controller: ExploreController = Depends(get_explore_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Explore a dataset version and generate profiling"""
    # Use soeid from the CurrentUser object
    # In a real implementation, you might want to map this to a user ID in your database
    return await controller.explore_dataset(
        dataset_id=dataset_id,
        version_id=version_id,
        request=request,
        user_id=current_user.role_id  # Using role_id as a simple user identifier for now
    )

@router.post(
    "/eda/{dataset_id}/versions/{version_id}/analyze",
    response_model=EDAResponse,
    summary="Perform comprehensive EDA analysis",
    description="""
    Perform Exploratory Data Analysis (EDA) on a dataset version.
    
    This endpoint provides:
    - Global dataset statistics
    - Variable-level analysis (numeric, categorical, datetime, text)
    - Interaction analysis (correlations, associations)
    - Missing value patterns
    - Data quality alerts
    
    All analysis results are returned as self-describing "Analysis Blocks"
    with explicit rendering instructions for the frontend.
    """
)
async def analyze_dataset_eda(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    request: EDARequest = Body(default_factory=EDARequest, description="EDA analysis configuration"),
    eda_service: EDAService = Depends(get_eda_service),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Perform comprehensive EDA analysis on a dataset version"""
    try:
        result = await eda_service.analyze_dataset(
            dataset_id=dataset_id,
            version_id=version_id,
            config=request.analysis_config
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error performing EDA analysis: {str(e)}"
        )

@router.get(
    "/eda/{dataset_id}/versions/{version_id}/stream",
    summary="Stream EDA analysis results",
    description="""
    Stream Exploratory Data Analysis (EDA) results as Server-Sent Events.
    
    This endpoint provides real-time streaming of analysis results as they are computed:
    - Each analysis block is sent as a separate event
    - Clients can display results progressively
    - Connection can be closed early to cancel analysis
    
    Event format:
    - event: analysis_block
    - data: JSON-encoded AnalysisBlock
    
    Special events:
    - event: metadata (sent first with dataset info)
    - event: complete (sent when analysis is done)
    - event: error (sent on analysis error)
    """
)
async def stream_eda_analysis(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    eda_service: EDAService = Depends(get_eda_service),
    current_user: CurrentUser = Depends(get_current_user_info),
    sample_size: Optional[int] = Query(None, description="Sample size for analysis"),
    variable_limit: Optional[int] = Query(20, description="Maximum variables to analyze"),
    correlation_limit: Optional[int] = Query(50, description="Maximum variables for correlation"),
):
    """Stream EDA analysis results using Server-Sent Events"""
    # Build analysis config from query parameters
    config = AnalysisConfig()
    if sample_size:
        config.sample_size = sample_size
    if variable_limit:
        config.variables.limit = variable_limit
    if correlation_limit:
        config.interactions.correlation_variable_limit = correlation_limit
    
    try:
        return StreamingResponse(
            eda_service.analyze_dataset_stream(
                dataset_id=dataset_id,
                version_id=version_id,
                config=config
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error starting EDA stream: {str(e)}"
        )