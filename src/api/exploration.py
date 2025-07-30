"""API endpoints for dataset exploration and profiling."""

import json
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, Query, Path
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime

from ..infrastructure.postgres.database import DatabasePool
from ..infrastructure.postgres.uow import PostgresUnitOfWork
from ..core.authorization import get_current_user_info, require_dataset_read
from ..core.domain_exceptions import resource_not_found
from ..core.domain_exceptions import ValidationException, BusinessRuleViolation
from ..api.models import CurrentUser
from .dependencies import get_db_pool, get_uow


router = APIRouter(prefix="/exploration", tags=["exploration"])


# Request/Response Models
class ProfileConfig(BaseModel):
    """Configuration for pandas profiling."""
    minimal: bool = Field(False, description="Use minimal profiling for faster results")
    samples_head: int = Field(10, ge=1, le=100, description="Number of head samples")
    samples_tail: int = Field(10, ge=1, le=100, description="Number of tail samples")
    missing_diagrams: bool = Field(True, description="Include missing value diagrams")
    correlation_threshold: float = Field(0.9, ge=0, le=1, description="Correlation threshold")
    n_obs: Optional[int] = Field(None, description="Number of observations to sample")


class CreateExplorationRequest(BaseModel):
    """Request to create an exploration job."""
    source_ref: str = Field("main", description="Source ref/branch name")
    table_key: str = Field("primary", description="Table to explore")
    profile_config: Optional[ProfileConfig] = Field(None, description="Profiling configuration")


class ExplorationJobResponse(BaseModel):
    """Response for exploration job creation."""
    job_id: str
    status: str
    message: str


class ExplorationHistoryItem(BaseModel):
    """Single item in exploration history."""
    job_id: str
    dataset_id: int
    dataset_name: str
    user_id: int
    username: str
    status: str
    created_at: str  # ISO format datetime string
    updated_at: Optional[str]  # ISO format datetime string
    run_parameters: Dict[str, Any]
    has_result: bool


class ExplorationHistoryResponse(BaseModel):
    """Response for exploration history."""
    items: List[ExplorationHistoryItem]
    total: int
    offset: int
    limit: int


@router.post("/datasets/{dataset_id}/jobs", response_model=ExplorationJobResponse)
async def create_exploration_job(
    dataset_id: int = Path(..., description="Dataset ID"),
    request: CreateExplorationRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    _: CurrentUser = Depends(require_dataset_read),
    uow: PostgresUnitOfWork = Depends(get_uow),
    pool: DatabasePool = Depends(get_db_pool)
) -> ExplorationJobResponse:
    """Create a new exploration/profiling job."""
    from ..features.exploration.services import ExplorationService
    from ..features.exploration.models import (
        CreateExplorationJobCommand,
        ProfileConfig as HandlerProfileConfig
    )
    
    # Convert profile config if provided
    profile_config = None
    if request.profile_config:
        profile_config = HandlerProfileConfig(
            minimal=request.profile_config.minimal,
            samples_head=request.profile_config.samples_head,
            samples_tail=request.profile_config.samples_tail,
            missing_diagrams=request.profile_config.missing_diagrams,
            correlation_threshold=request.profile_config.correlation_threshold,
            n_obs=request.profile_config.n_obs
        )
    
    # Create command
    command = CreateExplorationJobCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id,
        source_ref=request.source_ref,
        table_key=request.table_key,
        profile_config=profile_config
    )
    
    # Create service and execute
    service = ExplorationService(uow)
    return await service.create_exploration_job(command)


@router.get("/datasets/{dataset_id}/history", response_model=ExplorationHistoryResponse)
async def get_dataset_exploration_history(
    dataset_id: int = Path(..., description="Dataset ID"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    current_user: CurrentUser = Depends(get_current_user_info),
    _: CurrentUser = Depends(require_dataset_read),
    uow: PostgresUnitOfWork = Depends(get_uow)
) -> ExplorationHistoryResponse:
    """Get exploration history for a dataset."""
    from ..features.exploration.services import ExplorationService
    
    # Create service and execute
    service = ExplorationService(uow)
    result = await service.get_exploration_history(
        dataset_id=dataset_id,
        user_id=current_user.user_id,
        limit=limit,
        offset=offset
    )
    
    # Convert service response to API response
    return ExplorationHistoryResponse(
        items=[
            ExplorationHistoryItem(
                job_id=item.job_id,
                dataset_id=item.dataset_id,
                dataset_name=item.dataset_name,
                user_id=item.user_id,
                username=item.username,
                status=item.status,
                created_at=item.created_at,
                updated_at=item.updated_at,
                run_parameters=item.run_parameters,
                has_result=item.has_result
            )
            for item in result.items
        ],
        total=result.total,
        offset=result.offset,
        limit=result.limit
    )


@router.get("/users/{user_id}/history", response_model=ExplorationHistoryResponse)
async def get_user_exploration_history(
    user_id: int = Path(..., description="User ID"),
    dataset_id: Optional[int] = Query(None, description="Filter by dataset"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow)
) -> ExplorationHistoryResponse:
    """Get exploration history for a user."""
    from ..features.exploration.services import ExplorationService
    
    # Check permissions (users can only see their own history unless admin)
    if user_id != current_user.user_id and not current_user.is_admin():
        raise PermissionError("Cannot view other users' history")
    
    # Create service and execute
    service = ExplorationService(uow)
    result = await service.get_exploration_history(
        dataset_id=dataset_id,
        user_id=user_id,
        limit=limit,
        offset=offset
    )
    
    # Convert service response to API response
    return ExplorationHistoryResponse(
        items=[
            ExplorationHistoryItem(
                job_id=item.job_id,
                dataset_id=item.dataset_id,
                dataset_name=item.dataset_name,
                user_id=item.user_id,
                username=item.username,
                status=item.status,
                created_at=item.created_at,
                updated_at=item.updated_at,
                run_parameters=item.run_parameters,
                has_result=item.has_result
            )
            for item in result.items
        ],
        total=result.total,
        offset=result.offset,
        limit=result.limit
    )



@router.get("/jobs/{job_id}/result")
async def get_exploration_result(
    job_id: UUID = Path(..., description="Job ID"),
    format: str = Query("html", description="Output format (html, json, info)"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow)
):
    """Get the result of a completed exploration job."""
    from ..features.exploration.services import ExplorationService
    
    # Create service and execute
    service = ExplorationService(uow)
    result = await service.get_exploration_result(
        job_id=job_id,
        user_id=current_user.user_id,
        format=format
    )
    
    # Return appropriate response based on format
    if format == "html":
        return HTMLResponse(content=result["content"])
    else:  # json or info
        return JSONResponse(content=result["content"])