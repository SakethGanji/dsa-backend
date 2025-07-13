"""API endpoints for SQL workbench functionality."""

from fastapi import APIRouter, Depends
from typing import Dict

from ..core.database import DatabasePool
from ..core.abstractions.uow import IUnitOfWork
from ..core.abstractions.services import IWorkbenchService
from ..core.abstractions.repositories import ITableReader, IDatasetRepository, IJobRepository, ICommitRepository
from ..core.authorization import get_current_user_info
# Custom exception classes will be handled as standard Python exceptions
from ..models.pydantic_models import CurrentUser
from ..features.sql_workbench.models.sql_preview import SqlPreviewRequest, SqlPreviewResponse
from ..features.sql_workbench.models.sql_transform import SqlTransformRequest, SqlTransformResponse
from ..features.sql_workbench.handlers.preview_sql import PreviewSqlHandler
from ..features.sql_workbench.handlers.transform_sql import TransformSqlHandler
from ..core.dependencies import get_db_pool, get_uow

router = APIRouter(prefix="/workbench", tags=["workbench"])


# Local dependency helpers
async def get_workbench_service(
    uow: IUnitOfWork = Depends(get_uow)
) -> IWorkbenchService:
    """Get workbench service."""
    from ..core.services.workbench_service import WorkbenchService
    return WorkbenchService()


async def get_table_reader(
    uow: IUnitOfWork = Depends(get_uow)
) -> ITableReader:
    """Get table reader."""
    from ..infrastructure.postgres.table_reader import PostgresTableReader
    return PostgresTableReader(uow.connection)


async def get_dataset_repository(
    uow: IUnitOfWork = Depends(get_uow)
) -> IDatasetRepository:
    """Get dataset repository."""
    return uow.datasets


async def get_job_repository(
    uow: IUnitOfWork = Depends(get_uow)
) -> IJobRepository:
    """Get job repository."""
    return uow.jobs


async def get_commit_repository(
    uow: IUnitOfWork = Depends(get_uow)
) -> ICommitRepository:
    """Get commit repository."""
    return uow.commits


@router.post("/sql-preview", response_model=SqlPreviewResponse)
async def preview_sql(
    request: SqlPreviewRequest,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: IUnitOfWork = Depends(get_uow),
    workbench_service: IWorkbenchService = Depends(get_workbench_service),
    table_reader: ITableReader = Depends(get_table_reader),
    dataset_repository: IDatasetRepository = Depends(get_dataset_repository)
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
    handler = PreviewSqlHandler(uow, workbench_service, table_reader, dataset_repository)
    return await handler.handle(request, current_user.user_id)


@router.post("/sql-transform", response_model=SqlTransformResponse)
async def transform_sql(
    request: SqlTransformRequest,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: IUnitOfWork = Depends(get_uow),
    workbench_service: IWorkbenchService = Depends(get_workbench_service),
    job_repository: IJobRepository = Depends(get_job_repository),
    dataset_repository: IDatasetRepository = Depends(get_dataset_repository),
    commit_repository: ICommitRepository = Depends(get_commit_repository)
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
    handler = TransformSqlHandler(
        uow, 
        workbench_service, 
        job_repository, 
        dataset_repository,
        commit_repository
    )
    return await handler.handle(request, current_user.user_id)


# TODO: Add query history endpoint
# @router.get("/query-history")
# async def get_query_history(
#     offset: int = 0,
#     limit: int = 20,
#     current_user: Dict = Depends(get_current_user),
#     uow: IUnitOfWork = Depends(get_uow)
# ):
#     """Get user's SQL query history."""
#     pass