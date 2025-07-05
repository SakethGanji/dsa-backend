from fastapi import APIRouter, Depends, Query, File, UploadFile, Form
from typing import List, Dict, Any

from src.models.pydantic_models import (
    CreateCommitRequest, CreateCommitResponse,
    GetDataRequest, GetDataResponse,
    CommitSchemaResponse, QueueImportRequest, QueueImportResponse
)
from src.features.versioning.create_commit import CreateCommitHandler
from src.features.versioning.get_data_at_ref import GetDataAtRefHandler
from src.features.versioning.get_commit_schema import GetCommitSchemaHandler
from src.features.versioning.get_table_data import (
    GetTableDataHandler, ListTablesHandler, GetTableSchemaHandler
)
from src.features.versioning.queue_import_job import QueueImportJobHandler
from src.core.dependencies import get_uow, get_current_user
from src.core.abstractions import IUnitOfWork


router = APIRouter(tags=["versioning"])


@router.post("/datasets/{dataset_id}/refs/{ref_name}/commits", response_model=CreateCommitResponse)
async def create_commit(
    dataset_id: int,
    ref_name: str,
    request: CreateCommitRequest,
    current_user: dict = Depends(get_current_user),
    uow: IUnitOfWork = Depends(get_uow)
):
    """Create a new commit with direct data"""
    handler = CreateCommitHandler(uow)
    return await handler.handle(dataset_id, ref_name, request, current_user["id"])


@router.post("/datasets/{dataset_id}/refs/{ref_name}/import", response_model=QueueImportResponse)
async def import_file(
    dataset_id: int,
    ref_name: str,
    file: UploadFile = File(...),
    commit_message: str = Form(...),
    current_user: dict = Depends(get_current_user),
    uow: IUnitOfWork = Depends(get_uow)
):
    """Upload a file to import as a new commit"""
    handler = QueueImportJobHandler(uow)
    request = QueueImportRequest(commit_message=commit_message)
    return await handler.handle(
        dataset_id=dataset_id,
        ref_name=ref_name,
        file=file.file,
        filename=file.filename,
        request=request,
        user_id=current_user["id"]
    )


@router.get("/datasets/{dataset_id}/refs/{ref_name}/data", response_model=GetDataResponse)
async def get_data_at_ref(
    dataset_id: int,
    ref_name: str,
    sheet_name: str = Query(None, description="Filter by sheet name"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=1000, description="Pagination limit"),
    current_user: dict = Depends(get_current_user),
    uow: IUnitOfWork = Depends(get_uow)
):
    """Get paginated data for a ref"""
    handler = GetDataAtRefHandler(uow)
    request = GetDataRequest(sheet_name=sheet_name, offset=offset, limit=limit)
    return await handler.handle(dataset_id, ref_name, request, current_user["id"])


@router.get("/datasets/{dataset_id}/commits/{commit_id}/schema", response_model=CommitSchemaResponse)
async def get_commit_schema(
    dataset_id: int,
    commit_id: str,
    current_user: dict = Depends(get_current_user),
    uow: IUnitOfWork = Depends(get_uow)
):
    """Get schema information for a commit"""
    handler = GetCommitSchemaHandler(uow)
    return await handler.handle(dataset_id, commit_id, current_user["id"])


# Table-specific endpoints for multi-table support
@router.get("/datasets/{dataset_id}/refs/{ref_name}/tables")
async def list_tables(
    dataset_id: int,
    ref_name: str,
    current_user: dict = Depends(get_current_user),
    uow: IUnitOfWork = Depends(get_uow)
) -> Dict[str, List[str]]:
    """List all available tables in the dataset at the given ref"""
    handler = ListTablesHandler(uow, uow.table_reader)
    return await handler.handle(dataset_id, ref_name, current_user["id"])


@router.get("/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/data")
async def get_table_data(
    dataset_id: int,
    ref_name: str,
    table_key: str,
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=10000, description="Pagination limit"),
    current_user: dict = Depends(get_current_user),
    uow: IUnitOfWork = Depends(get_uow)
) -> Dict[str, Any]:
    """Get paginated data for a specific table"""
    handler = GetTableDataHandler(uow, uow.table_reader)
    return await handler.handle(
        dataset_id=dataset_id,
        ref_name=ref_name,
        table_key=table_key,
        user_id=current_user["id"],
        offset=offset,
        limit=limit
    )


@router.get("/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/schema")
async def get_table_schema(
    dataset_id: int,
    ref_name: str,
    table_key: str,
    current_user: dict = Depends(get_current_user),
    uow: IUnitOfWork = Depends(get_uow)
) -> Dict[str, Any]:
    """Get schema for a specific table"""
    handler = GetTableSchemaHandler(uow, uow.table_reader)
    return await handler.handle(
        dataset_id=dataset_id,
        ref_name=ref_name,
        table_key=table_key,
        user_id=current_user["id"]
    )