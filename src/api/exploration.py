"""API endpoints for dataset exploration and profiling."""

import json
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, Query, Path
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime

from ..core.database import DatabasePool
from ..core.abstractions.uow import IUnitOfWork
from ..core.authorization import get_current_user_info, require_dataset_read
from ..core.exceptions import resource_not_found
from ..core.domain_exceptions import ValidationException, BusinessRuleViolation
from ..models.pydantic_models import CurrentUser
from ..core.dependencies import get_db_pool, get_uow


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
    uow: IUnitOfWork = Depends(get_uow),
    pool: DatabasePool = Depends(get_db_pool)
) -> ExplorationJobResponse:
    """Create a new exploration/profiling job.\""""
    
    # Get current commit for ref
    ref = await uow.commits.get_ref(dataset_id, request.source_ref)
    if not ref:
        raise resource_not_found("Ref", request.source_ref)
    
    source_commit_id = ref['commit_id']
    
    # Convert profile config to dict
    profile_config = None
    if request.profile_config:
        profile_config = {
            "minimal": request.profile_config.minimal,
            "samples": {
                "head": request.profile_config.samples_head,
                "tail": request.profile_config.samples_tail
            },
            "missing_diagrams": {
                "bar": request.profile_config.missing_diagrams,
                "matrix": request.profile_config.missing_diagrams
            },
            "correlations": {
                "pearson": {
                    "calculate": True,
                    "threshold": request.profile_config.correlation_threshold
                }
            }
        }
        
        if request.profile_config.n_obs:
            profile_config["n_obs"] = request.profile_config.n_obs
    
    # Create exploration job using existing job infrastructure
    job_params = {
        "table_key": request.table_key,
        "profile_config": profile_config or {},
        "output_format": "html"
    }
    
    job_id = await uow.jobs.create_job(
        run_type="exploration",
        dataset_id=dataset_id,
        user_id=current_user.user_id,
        source_commit_id=source_commit_id,
        run_parameters=job_params
    )
    
    await uow.commit()
    
    return ExplorationJobResponse(
        job_id=str(job_id),
        status="pending",
        message="Exploration job created successfully"
    )


@router.get("/datasets/{dataset_id}/history", response_model=ExplorationHistoryResponse)
async def get_dataset_exploration_history(
    dataset_id: int = Path(..., description="Dataset ID"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    current_user: CurrentUser = Depends(get_current_user_info),
    _: CurrentUser = Depends(require_dataset_read),
    uow: IUnitOfWork = Depends(get_uow),
    pool: DatabasePool = Depends(get_db_pool)
) -> ExplorationHistoryResponse:
    """Get exploration history for a dataset.\""""
    
    # Get history using direct query
    async with pool.acquire() as conn:
        query = """
            SELECT 
                ar.id as job_id,
                ar.dataset_id,
                ar.user_id,
                ar.status,
                ar.created_at,
                ar.completed_at as updated_at,
                ar.run_parameters,
                ar.output_summary,
                d.name as dataset_name,
                u.soeid as username
            FROM dsa_jobs.analysis_runs ar
            JOIN dsa_core.datasets d ON ar.dataset_id = d.id
            JOIN dsa_auth.users u ON ar.user_id = u.id
            WHERE ar.run_type = 'exploration' AND ar.dataset_id = $1
            ORDER BY ar.created_at DESC
            OFFSET $2 LIMIT $3
        """
        
        rows = await conn.fetch(query, dataset_id, offset, limit)
        
        items = [
            ExplorationHistoryItem(
                job_id=str(row["job_id"]),
                dataset_id=row["dataset_id"],
                dataset_name=row["dataset_name"],
                user_id=row["user_id"],
                username=row["username"],
                status=row["status"],
                created_at=row["created_at"].isoformat(),
                updated_at=row["updated_at"].isoformat() if row["updated_at"] else None,
                run_parameters=json.loads(row["run_parameters"]) if isinstance(row["run_parameters"], str) else row["run_parameters"] or {},
                has_result=bool(row["output_summary"])
            )
            for row in rows
        ]
    
    # Get total count
    async with pool.acquire() as conn:
        count_query = """
            SELECT COUNT(*) 
            FROM dsa_jobs.analysis_runs 
            WHERE run_type = 'exploration' AND dataset_id = $1
        """
        total = await conn.fetchval(count_query, dataset_id)
    
    return ExplorationHistoryResponse(
        items=items,
        total=total,
        offset=offset,
        limit=limit
    )


@router.get("/users/{user_id}/history", response_model=ExplorationHistoryResponse)
async def get_user_exploration_history(
    user_id: int = Path(..., description="User ID"),
    dataset_id: Optional[int] = Query(None, description="Filter by dataset"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    current_user: CurrentUser = Depends(get_current_user_info),
    pool: DatabasePool = Depends(get_db_pool)
) -> ExplorationHistoryResponse:
    """Get exploration history for a user."""
    
    # Check permissions (users can only see their own history unless admin)
    if user_id != current_user.user_id and not current_user.is_admin():
        raise PermissionError("Cannot view other users' history")
    
    # Get history using direct query
    async with pool.acquire() as conn:
        query = """
            SELECT 
                ar.id as job_id,
                ar.dataset_id,
                ar.user_id,
                ar.status,
                ar.created_at,
                ar.completed_at as updated_at,
                ar.run_parameters,
                ar.output_summary,
                d.name as dataset_name,
                u.soeid as username
            FROM dsa_jobs.analysis_runs ar
            JOIN dsa_core.datasets d ON ar.dataset_id = d.id
            JOIN dsa_auth.users u ON ar.user_id = u.id
            WHERE ar.run_type = 'exploration' AND ar.user_id = $1
        """
        
        params = [user_id]
        
        if dataset_id:
            query += " AND ar.dataset_id = $2"
            params.append(dataset_id)
            
        query += f" ORDER BY ar.created_at DESC OFFSET ${len(params) + 1} LIMIT ${len(params) + 2}"
        params.extend([offset, limit])
        
        rows = await conn.fetch(query, *params)
        
        items = [
            ExplorationHistoryItem(
                job_id=str(row["job_id"]),
                dataset_id=row["dataset_id"],
                dataset_name=row["dataset_name"],
                user_id=row["user_id"],
                username=row["username"],
                status=row["status"],
                created_at=row["created_at"].isoformat(),
                updated_at=row["updated_at"].isoformat() if row["updated_at"] else None,
                run_parameters=json.loads(row["run_parameters"]) if isinstance(row["run_parameters"], str) else row["run_parameters"] or {},
                has_result=bool(row["output_summary"])
            )
            for row in rows
        ]
    
    # Get total count
    async with pool.acquire() as conn:
        count_query = """
            SELECT COUNT(*) 
            FROM dsa_jobs.analysis_runs 
            WHERE run_type = 'exploration' AND user_id = $1
        """
        params = [user_id]
        
        if dataset_id:
            count_query += " AND dataset_id = $2"
            params.append(dataset_id)
        
        total = await conn.fetchval(count_query, *params)
    
    return ExplorationHistoryResponse(
        items=items,
        total=total,
        offset=offset,
        limit=limit
    )



@router.get("/jobs/{job_id}/result")
async def get_exploration_result(
    job_id: UUID = Path(..., description="Job ID"),
    format: str = Query("html", description="Output format (html, json, info)"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: IUnitOfWork = Depends(get_uow),
    pool: DatabasePool = Depends(get_db_pool)
):
    """Get the result of a completed exploration job."""
    
    # Get job details
    job = await uow.jobs.get_job_by_id(job_id)
    if not job:
        raise resource_not_found("Job", job_id)
    
    if job["run_type"] != "exploration":
        raise ValidationException("Not an exploration job", field="run_type")
    
    if job["status"] != "completed":
        raise BusinessRuleViolation(f"Job is {job['status']}, not completed", rule="job_must_be_completed")
    
    # Permission check will be handled by the handler
    
    # Get result from output_summary
    async with pool.acquire() as conn:
        query = """
            SELECT output_summary
            FROM dsa_jobs.analysis_runs
            WHERE id = $1 AND run_type = 'exploration' AND status = 'completed'
        """
        
        row = await conn.fetchrow(query, job_id)
        
        if not row or not row["output_summary"]:
            raise resource_not_found("Result", job_id)
        
        output_summary = json.loads(row["output_summary"])
        
        # Return appropriate response based on format
        if format == "html":
            return HTMLResponse(content=output_summary.get("profile_html", ""))
        elif format == "json":
            return JSONResponse(content=json.loads(output_summary.get("profile_json", "{}")))
        else:  # info
            return JSONResponse(content=output_summary.get("dataset_info", {}))