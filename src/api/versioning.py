from fastapi import APIRouter, Depends, Query, File, UploadFile, Form, status, Path
from typing import List, Dict, Any, Optional

from src.api.models import (
    CreateCommitRequest, CreateCommitResponse,
    GetDataRequest, GetDataResponse,
    CommitSchemaResponse, QueueImportRequest, QueueImportResponse,
    GetCommitHistoryResponse, CheckoutResponse, CurrentUser,
    ListRefsResponse, CreateBranchRequest, CreateBranchResponse,
    TableAnalysisResponse, DatasetOverviewResponse
)
from src.features.versioning.handlers.create_commit import CreateCommitHandler
from src.features.versioning.handlers.get_data_at_ref import GetDataAtRefHandler
from src.features.versioning.handlers.get_commit_schema import GetCommitSchemaHandler
from src.features.versioning.handlers.get_table_data import (
    GetTableDataHandler, ListTablesHandler, GetTableSchemaHandler
)
from src.features.versioning.handlers.get_table_analysis import GetTableAnalysisHandler
from src.features.versioning.handlers.queue_import_job import QueueImportJobHandler
from src.features.versioning.handlers.get_commit_history import GetCommitHistoryHandler
from src.features.versioning.handlers.checkout_commit import CheckoutCommitHandler
from src.features.versioning.handlers.get_dataset_overview import GetDatasetOverviewHandler
from src.features.refs.handlers import ListRefsHandler, CreateBranchHandler, DeleteBranchHandler
from src.infrastructure.postgres.database import DatabasePool, UnitOfWorkFactory
from src.core.authorization import get_current_user_info, PermissionType, require_dataset_read, require_dataset_write
from src.api.dependencies import get_uow, get_db_pool, get_event_bus, get_permission_service
from src.infrastructure.postgres.uow import PostgresUnitOfWork


router = APIRouter(tags=["versioning"])


# Dependency injection helpers
async def get_uow_factory(
    pool: DatabasePool = Depends(get_db_pool)
) -> UnitOfWorkFactory:
    """Get unit of work factory."""
    return UnitOfWorkFactory(pool)


async def get_table_analysis_service(
    uow: PostgresUnitOfWork = Depends(get_uow)
):
    """Get table analysis service."""
    from src.infrastructure.services.table_analysis import (
        TableAnalysisService, DataTypeInferenceService, ColumnStatisticsService
    )
    return TableAnalysisService(
        table_reader=uow.table_reader,
        type_inference_service=DataTypeInferenceService(),
        statistics_service=ColumnStatisticsService()
    )


@router.post("/datasets/{dataset_id}/refs/{ref_name}/commits", response_model=CreateCommitResponse)
async def create_commit(
    dataset_id: int,
    ref_name: str,
    request: CreateCommitRequest,
    current_user: CurrentUser = Depends(get_current_user_info),
    _: CurrentUser = Depends(require_dataset_write),
    uow: PostgresUnitOfWork = Depends(get_uow),
    event_bus = Depends(get_event_bus),
    permission_service = Depends(get_permission_service)
):
    """Create a new commit with direct data"""
    handler = CreateCommitHandler(uow, uow.commits, uow.datasets, permissions=permission_service, event_bus=event_bus)
    return await handler.handle(dataset_id, ref_name, request, current_user.user_id)


@router.post("/datasets/{dataset_id}/refs/{ref_name}/import", response_model=QueueImportResponse)
async def import_file(
    dataset_id: int,
    ref_name: str,
    file: UploadFile = File(...),
    commit_message: str = Form(...),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_write)
):
    """Upload a file to import as a new commit"""
    handler = QueueImportJobHandler(uow, permissions=permission_service)
    request = QueueImportRequest(commit_message=commit_message)
    return await handler.handle(
        dataset_id=dataset_id,
        ref_name=ref_name,
        file=file.file,
        filename=file.filename,
        request=request,
        user_id=current_user.user_id
    )


@router.get("/datasets/{dataset_id}/refs/{ref_name}/data", response_model=GetDataResponse)
async def get_data_at_ref(
    dataset_id: int,
    ref_name: str,
    table_key: str = Query(None, description="Filter by specific table/sheet"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=1000, description="Pagination limit"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_read)
):
    """Get paginated data for a ref"""
    handler = GetDataAtRefHandler(uow, uow.table_reader, permissions=permission_service)
    request = GetDataRequest(sheet_name=table_key, offset=offset, limit=limit)
    return await handler.handle(dataset_id, ref_name, request, current_user.user_id)


@router.get("/datasets/{dataset_id}/commits/{commit_id}/schema", response_model=CommitSchemaResponse)
async def get_commit_schema(
    dataset_id: int,
    commit_id: str,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_read)
):
    """Get schema information for a commit"""
    handler = GetCommitSchemaHandler(uow.commits, uow.datasets, permissions=permission_service)
    return await handler.handle(dataset_id, commit_id, current_user.user_id)


# Table-specific endpoints for multi-table support
@router.get("/datasets/{dataset_id}/refs/{ref_name}/tables")
async def list_tables(
    dataset_id: int,
    ref_name: str,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_read)
) -> Dict[str, List[str]]:
    """List all available tables in the dataset at the given ref"""
    handler = ListTablesHandler(uow, uow.table_reader, permissions=permission_service)
    return await handler.handle(dataset_id, ref_name, current_user.user_id)


@router.get("/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/data")
async def get_table_data(
    dataset_id: int,
    ref_name: str,
    table_key: str,
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=10000, description="Pagination limit"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_read)
) -> Dict[str, Any]:
    """Get paginated data for a specific table"""
    handler = GetTableDataHandler(uow, uow.table_reader, permissions=permission_service)
    return await handler.handle(
        dataset_id=dataset_id,
        ref_name=ref_name,
        table_key=table_key,
        user_id=current_user.user_id,
        offset=offset,
        limit=limit
    )


@router.get("/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/schema")
async def get_table_schema(
    dataset_id: int,
    ref_name: str,
    table_key: str,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_read)
) -> Dict[str, Any]:
    """Get schema for a specific table"""
    handler = GetTableSchemaHandler(uow, uow.table_reader, permissions=permission_service)
    return await handler.handle(
        dataset_id=dataset_id,
        ref_name=ref_name,
        table_key=table_key,
        user_id=current_user.user_id
    )


@router.get("/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/analysis", response_model=TableAnalysisResponse)
async def get_table_analysis(
    dataset_id: int,
    ref_name: str,
    table_key: str,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    table_analysis_service = Depends(get_table_analysis_service),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_read)
) -> TableAnalysisResponse:
    """Get comprehensive table analysis including schema, statistics, and sample values"""
    handler = GetTableAnalysisHandler(uow, table_analysis_service, permissions=permission_service)
    return await handler.handle(
        dataset_id=dataset_id,
        ref_name=ref_name,
        table_key=table_key,
        user_id=current_user.user_id
    )


# New endpoints for commit history and checkout
@router.get("/datasets/{dataset_id}/history", response_model=GetCommitHistoryResponse)
async def get_commit_history(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: str = Query("main", description="Ref/branch name to get history for"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(50, ge=1, le=100, description="Number of commits to return"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_read)
) -> GetCommitHistoryResponse:
    """Get the chronological commit history for a dataset."""
    # Get commit history
    uow = uow_factory.create()
    handler = GetCommitHistoryHandler(uow, permissions=permission_service)
    return await handler.handle(dataset_id, ref_name, offset, limit)


@router.get("/datasets/{dataset_id}/commits/{commit_id}/data", response_model=GetDataResponse)
async def checkout_commit(
    dataset_id: int = Path(..., description="Dataset ID"),
    commit_id: str = Path(..., description="Commit ID to checkout"),
    table_key: Optional[str] = Query(None, description="Specific table/sheet to retrieve"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to return"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_read)
) -> GetDataResponse:
    """Get the data as it existed at a specific commit."""
    # Checkout commit
    uow = uow_factory.create()
    handler = CheckoutCommitHandler(uow, permissions=permission_service)
    return await handler.handle(dataset_id, commit_id, table_key, offset, limit)


# Branch/Ref management endpoints
@router.get("/datasets/{dataset_id}/refs", response_model=ListRefsResponse)
async def list_refs(
    dataset_id: int = Path(..., description="Dataset ID"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_read)
) -> ListRefsResponse:
    """List all branches/refs for a dataset."""
    uow = uow_factory.create()
    handler = ListRefsHandler(uow, permissions=permission_service)
    return await handler.handle(dataset_id, current_user.user_id)


@router.post("/datasets/{dataset_id}/refs", response_model=CreateBranchResponse)
async def create_branch(
    dataset_id: int = Path(..., description="Dataset ID"),
    request: CreateBranchRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_write)
) -> CreateBranchResponse:
    """Create a new branch from an existing ref."""
    uow = uow_factory.create()
    handler = CreateBranchHandler(uow, permissions=permission_service)
    return await handler.handle(dataset_id, request, current_user.user_id)


@router.delete("/datasets/{dataset_id}/refs/{ref_name}")
async def delete_branch(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: str = Path(..., description="Branch/ref name to delete"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_write)
):
    """Delete a branch/ref."""
    from src.features.refs.handlers.delete_branch import DeleteBranchCommand, DeleteBranchResponse
    handler = DeleteBranchHandler(uow, permissions=permission_service)
    command = DeleteBranchCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id,
        ref_name=ref_name
    )
    return await handler.handle(command)


# Dataset Overview endpoint
@router.get("/datasets/{dataset_id}/overview", response_model=DatasetOverviewResponse)
async def get_dataset_overview(
    dataset_id: int = Path(..., description="Dataset ID"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service),
    _: CurrentUser = Depends(require_dataset_read)
) -> DatasetOverviewResponse:
    """Get overview of dataset including all refs and their tables.
    
    This endpoint provides everything the UI needs to populate ref_name and table_key
    dropdowns for the columns endpoint.
    """
    # Use optimized handler that does bulk fetching
    handler = GetDatasetOverviewHandler(uow, uow.table_reader, permissions=permission_service)
    return await handler.handle(dataset_id, current_user.user_id)