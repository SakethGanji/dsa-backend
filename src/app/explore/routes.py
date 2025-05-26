from fastapi import APIRouter, Depends, Path, Body, HTTPException, status
from typing import Dict, Any
from datetime import datetime

from app.explore.models import ExploreRequest
from app.explore.controller import ExploreController
from app.explore.service import ExploreService
from app.datasets.repository import DatasetsRepository
from app.db.connection import get_session
from app.users.models import UserOut as User
from sqlalchemy.ext.asyncio import AsyncSession

# Import the auth dependencies from users module
from app.users.auth import get_current_user_info, CurrentUser

# Create dependency for the controller
def get_explore_controller(session: AsyncSession = Depends(get_session)):
    repository = DatasetsRepository(session)
    service = ExploreService(repository)
    controller = ExploreController(service)
    return controller

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