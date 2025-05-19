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
    Apply operations to a dataset and generate a profile report or summary.

    Steps:
    1. Load the dataset
    2. Apply operations like filtering, column modification, and sampling
    3. Generate a summary or full profile report

    Supported operations:
    - filter_rows: Filter rows using a pandas query expression
    - sample_rows: Sample rows with various methods (random, head, tail)
    - remove_columns: Remove specified columns from the dataset
    - rename_columns: Rename columns using a mapping dictionary
    - remove_nulls: Remove rows with null values in specified columns
    - derive_column: Create a new column using an expression
    - sort_rows: Sort the data by specified columns with custom ordering
    """
)
async def explore_dataset(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    request: ExploreRequest = Body(..., description="Operations to apply to the dataset"),
    controller: ExploreController = Depends(get_explore_controller),
    current_user: CurrentUser = Depends(get_current_user_info)
):
    """Explore a dataset version with operations and profiling"""
    # Use soeid from the JWT token as the user identifier
    # In a real implementation, you'd map this to a user ID from your database
    return await controller.explore_dataset(
        dataset_id=dataset_id,
        version_id=version_id,
        request=request,
        user_id=1  # Mock user ID for the example
    )