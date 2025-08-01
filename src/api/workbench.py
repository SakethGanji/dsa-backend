"""API endpoints for SQL workbench functionality."""

from fastapi import APIRouter, Depends

from ..infrastructure.postgres.uow import PostgresUnitOfWork
from ..infrastructure.postgres.table_reader import PostgresTableReader
from ..infrastructure.postgres.dataset_repo import PostgresDatasetRepository
from ..infrastructure.postgres.job_repo import PostgresJobRepository
from ..infrastructure.postgres.versioning_repo import PostgresCommitRepository
from ..core.authorization import get_current_user_info
# Custom exception classes will be handled as standard Python exceptions
from ..api.models import CurrentUser
from ..features.sql_workbench.models.sql_preview import SqlPreviewRequest, SqlPreviewResponse
from ..features.sql_workbench.models.sql_transform import SqlTransformRequest, SqlTransformResponse
from ..features.sql_workbench.services.sql_workbench_service import SqlWorkbenchService
from .dependencies import get_uow, get_permission_service

router = APIRouter(prefix="/workbench", tags=["workbench"])


# Local dependency helpers
async def get_table_reader(
    uow: PostgresUnitOfWork = Depends(get_uow)
) -> PostgresTableReader:
    """Get table reader."""
    return PostgresTableReader(uow.connection)


async def get_dataset_repository(
    uow: PostgresUnitOfWork = Depends(get_uow)
) -> PostgresDatasetRepository:
    """Get dataset repository."""
    return uow.datasets


async def get_job_repository(
    uow: PostgresUnitOfWork = Depends(get_uow)
) -> PostgresJobRepository:
    """Get job repository."""
    return uow.jobs


async def get_commit_repository(
    uow: PostgresUnitOfWork = Depends(get_uow)
) -> PostgresCommitRepository:
    """Get commit repository."""
    return uow.commits


@router.post("/sql-preview", response_model=SqlPreviewResponse)
async def preview_sql(
    request: SqlPreviewRequest,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    table_reader: PostgresTableReader = Depends(get_table_reader),
    dataset_repository: PostgresDatasetRepository = Depends(get_dataset_repository),
    permission_service = Depends(get_permission_service)
):
    """
    Preview SQL query results on sample data.
    
    This endpoint executes the provided SQL query on sample data from the specified
    source tables and returns the results. The query is executed with a limit to
    ensure fast response times.
    
    Args:
        request: SQL preview request containing sources and query
        
    Returns:
        SqlPreviewResponse with query results and metadata
        
    Raises:
        400 for validation errors, 403 for permission errors, 404 for not found
    """
    service = SqlWorkbenchService(uow, permissions=permission_service)
    return await service.preview_sql(request, current_user.user_id)


@router.post("/sql-transform", response_model=SqlTransformResponse)
async def transform_sql(
    request: SqlTransformRequest,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    job_repository: PostgresJobRepository = Depends(get_job_repository),
    dataset_repository: PostgresDatasetRepository = Depends(get_dataset_repository),
    commit_repository: PostgresCommitRepository = Depends(get_commit_repository),
    permission_service = Depends(get_permission_service)
):
    """
    Create a SQL transformation job.
    
    This endpoint creates an asynchronous job that will execute the SQL transformation
    and save the results as a new commit in the target dataset. The job can be tracked
    using the returned job_id.
    
    Args:
        request: SQL transformation request containing sources, query, and target
        
    Returns:
        SqlTransformResponse with job ID for tracking
        
    Raises:
        400 for validation errors, 403 for permission errors, 404 for not found
    """
    service = SqlWorkbenchService(
        uow, 
        permissions=permission_service,
        sql_executor=None,  # Will use default SqlExecutor
        job_repository=job_repository,
        dataset_repository=dataset_repository,
        commit_repository=commit_repository
    )
    return await service.transform_sql(request, current_user.user_id)


