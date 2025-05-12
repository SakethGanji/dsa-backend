from fastapi import APIRouter, Depends, Path, Body, HTTPException, status
from typing import Dict, Any

from app.explore.models import ExploreRequest
from app.explore.controller import ExploreController
from app.explore.service import ExploreService
from app.datasets.repository import DatasetsRepository
from app.db.connection import get_session
from app.users.models import UserOut as User
from sqlalchemy.ext.asyncio import AsyncSession

# Re-use the authentication dependency from datasets module
async def get_current_user():
    # This is a mock implementation
    from datetime import datetime
    return User(
        id=1,
        soeid="mock_user",
        role_id=1,
        created_at=datetime.now(),
        updated_at=datetime.now()
    )

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
    response_model=Dict[str, Any]
)
async def explore_dataset(
    dataset_id: int = Path(..., description="The ID of the dataset"),
    version_id: int = Path(..., description="The ID of the version"),
    request: ExploreRequest = Body(...),
    controller: ExploreController = Depends(get_explore_controller),
    current_user: User = Depends(get_current_user)
):
    """
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

    Query parameters:
    - run_profiling: Set to 'true' to generate a full profile report (may be slow for large datasets)
                     Default is 'false' which returns just a quick summary
    - format: Output format for the profile report - 'json' or 'html'
              HTML gives a full interactive HTML report
              JSON gives structured data for programmatic use

    Returns:
    - Summary of the data or full profile report with statistics (based on run_profiling flag)
    """
    result = await controller.explore_dataset(
        dataset_id=dataset_id,
        version_id=version_id,
        request=request,
        user_id=current_user.id
    )
    
    return result