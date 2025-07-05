from fastapi import APIRouter, Depends, Query

from models.pydantic_models import (
    CreateCommitRequest, CreateCommitResponse,
    GetDataRequest, GetDataResponse,
    CommitSchemaResponse
)
from features.versioning.create_commit import CreateCommitHandler
from features.versioning.get_data_at_ref import GetDataAtRefHandler
from features.versioning.get_commit_schema import GetCommitSchemaHandler

# Dependency injection functions
async def get_current_user_id() -> int:
    # TODO: Extract from JWT token
    return 1

async def get_create_commit_handler() -> CreateCommitHandler:
    # TODO: Wire up dependencies
    pass

async def get_data_at_ref_handler() -> GetDataAtRefHandler:
    # TODO: Wire up dependencies
    pass

async def get_commit_schema_handler() -> GetCommitSchemaHandler:
    # TODO: Wire up dependencies
    pass


router = APIRouter(tags=["versioning"])


@router.post("/datasets/{dataset_id}/refs/{ref_name}/commits", response_model=CreateCommitResponse)
async def create_commit(
    dataset_id: int,
    ref_name: str,
    request: CreateCommitRequest,
    user_id: int = Depends(get_current_user_id),
    handler: CreateCommitHandler = Depends(get_create_commit_handler)
):
    """Create a new commit with direct data"""
    return await handler.handle(dataset_id, ref_name, request, user_id)


@router.get("/datasets/{dataset_id}/refs/{ref_name}/data", response_model=GetDataResponse)
async def get_data_at_ref(
    dataset_id: int,
    ref_name: str,
    sheet_name: str = Query(None, description="Filter by sheet name"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=1000, description="Pagination limit"),
    user_id: int = Depends(get_current_user_id),
    handler: GetDataAtRefHandler = Depends(get_data_at_ref_handler)
):
    """Get paginated data for a ref"""
    request = GetDataRequest(sheet_name=sheet_name, offset=offset, limit=limit)
    return await handler.handle(dataset_id, ref_name, request, user_id)


@router.get("/datasets/{dataset_id}/commits/{commit_id}/schema", response_model=CommitSchemaResponse)
async def get_commit_schema(
    dataset_id: int,
    commit_id: str,
    user_id: int = Depends(get_current_user_id),
    handler: GetCommitSchemaHandler = Depends(get_commit_schema_handler)
):
    """Get schema information for a commit"""
    return await handler.handle(dataset_id, commit_id, user_id)