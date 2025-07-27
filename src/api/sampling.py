"""API endpoints for data sampling operations."""

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, Query, Path, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, validator
from uuid import UUID
from datetime import datetime
import io
import csv

from ..infrastructure.postgres.database import DatabasePool, UnitOfWorkFactory
from ..infrastructure.postgres.uow import PostgresUnitOfWork
from ..infrastructure.services.sampling_service import SamplingService
from ..infrastructure.postgres.table_reader import PostgresTableReader
from enum import Enum
from ..core.authorization import get_current_user_info, require_dataset_read
from ..core.domain_exceptions import resource_not_found
from ..core.domain_exceptions import ValidationException
from ..api.models import CurrentUser
from ..features.sampling.handlers import (
    GetSamplingJobDataHandler,
    GetDatasetSamplingHistoryHandler,
    GetUserSamplingHistoryHandler
)
from .dependencies import get_db_pool, get_uow, get_permission_service


# Sampling method enum
class SamplingMethod(Enum):
    """Supported sampling methods."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    CLUSTER = "cluster"
    SYSTEMATIC = "systematic"


router = APIRouter(prefix="/sampling", tags=["sampling"])


# Local dependency helpers
async def get_table_reader(
    uow: PostgresUnitOfWork = Depends(get_uow)
) -> PostgresTableReader:
    """Get table reader."""
    return PostgresTableReader(uow.connection)


# Request/Response Models
class FilterCondition(BaseModel):
    """Single filter condition."""
    column: str = Field(..., description="Column name to filter on")
    operator: str = Field(..., description="Filter operator (>, <, =, in, etc.)")
    value: Any = Field(..., description="Value to compare against")


class FilterSpec(BaseModel):
    """Filter specification for sampling."""
    conditions: List[FilterCondition] = Field(default_factory=list)
    logic: str = Field("AND", description="Logic operator (AND/OR)")
    
    @validator('logic')
    def validate_logic(cls, v):
        if v.upper() not in ['AND', 'OR']:
            raise ValidationException("Logic must be AND or OR", field="logic")
        return v.upper()


class SelectionSpec(BaseModel):
    """Column selection and ordering specification."""
    columns: Optional[List[str]] = Field(None, description="Columns to include")
    order_by: Optional[str] = Field(None, description="Column to order by")
    order_desc: bool = Field(False, description="Order descending")


class SamplingRoundConfig(BaseModel):
    """Configuration for a single sampling round."""
    round_number: int = Field(..., description="Round number (1-based)")
    method: str = Field(..., description="Sampling method")
    parameters: Dict[str, Any] = Field(..., description="Method-specific parameters")
    output_name: Optional[str] = Field(None, description="Name for this round's output")
    filters: Optional[FilterSpec] = Field(None, description="Row filters for this round")
    selection: Optional[SelectionSpec] = Field(None, description="Column selection for this round")
    
    @validator('parameters')
    def validate_parameters(cls, v, values):
        """Validate method-specific parameters."""
        method = values.get('method')
        if not method:
            return v
            
        # Validate random sampling parameters
        if method == 'random':
            if 'sample_size' not in v:
                raise ValidationException("Random sampling requires 'sample_size' parameter", field="parameters.sample_size")
            if not isinstance(v['sample_size'], int) or v['sample_size'] <= 0:
                raise ValidationException("sample_size must be a positive integer", field="parameters.sample_size")
                
        # Validate stratified sampling parameters
        elif method == 'stratified':
            if 'strata_columns' not in v:
                raise ValidationException("Stratified sampling requires 'strata_columns' parameter", field="parameters.strata_columns")
            if not isinstance(v['strata_columns'], list) or not v['strata_columns']:
                raise ValidationException("strata_columns must be a non-empty list", field="parameters.strata_columns")
            
            # Check if it's disproportional (fixed-N) or proportional sampling
            if 'samples_per_stratum' in v:
                # Disproportional sampling - fixed number per stratum
                if not isinstance(v['samples_per_stratum'], int) or v['samples_per_stratum'] <= 0:
                    raise ValidationException("samples_per_stratum must be a positive integer", field="parameters.samples_per_stratum")
                # Ensure sample_size is not also provided (they're mutually exclusive)
                if 'sample_size' in v:
                    raise ValidationException("Cannot specify both 'sample_size' and 'samples_per_stratum'. Use 'samples_per_stratum' for disproportional sampling or 'sample_size' for proportional sampling", field="parameters")
            else:
                # Proportional sampling - requires sample_size
                if 'sample_size' not in v:
                    raise ValidationException("Stratified sampling requires either 'sample_size' (proportional) or 'samples_per_stratum' (disproportional) parameter", field="parameters.sample_size")
                
        # Validate systematic sampling parameters
        elif method == 'systematic':
            if 'interval' not in v:
                raise ValidationException("Systematic sampling requires 'interval' parameter", field="parameters.interval")
            if not isinstance(v['interval'], int) or v['interval'] <= 0:
                raise ValidationException("interval must be a positive integer", field="parameters.interval")
                
        # Validate cluster sampling parameters
        elif method == 'cluster':
            if 'cluster_column' not in v:
                raise ValidationException("Cluster sampling requires 'cluster_column' parameter", field="parameters.cluster_column")
            if 'num_clusters' not in v:
                raise ValidationException("Cluster sampling requires 'num_clusters' parameter", field="parameters.num_clusters")
                
        return v


class CreateSamplingJobRequest(BaseModel):
    """Request to create a sampling job."""
    source_ref: str = Field("main", description="Source ref/branch name")
    table_key: str = Field("primary", description="Table to sample from")
    output_branch_name: Optional[str] = Field(None, description="Name for the output branch (defaults to commit ID)")
    commit_message: Optional[str] = Field(None, description="Message for output commit")
    rounds: List[SamplingRoundConfig] = Field(..., description="Sampling rounds to execute")
    export_residual: bool = Field(False, description="Export unsampled records")
    residual_output_name: Optional[str] = Field(None, description="Name for residual output")


class DirectSamplingRequest(BaseModel):
    """Request for direct sampling (non-job based)."""
    method: SamplingMethod
    sample_size: int = Field(..., gt=0, description="Number of samples")
    random_seed: Optional[int] = Field(None, description="Random seed for reproducibility")
    
    # Method-specific parameters
    stratify_columns: Optional[List[str]] = Field(None, description="Columns for stratification")
    proportional: bool = Field(True, description="Use proportional allocation")
    samples_per_stratum: Optional[int] = Field(None, description="Fixed samples per stratum (disproportional sampling)")
    cluster_column: Optional[str] = Field(None, description="Column for clustering")
    num_clusters: Optional[int] = Field(None, description="Number of clusters to select")
    
    # Filtering and selection
    filters: Optional[FilterSpec] = Field(None, description="Row filters")
    selection: Optional[SelectionSpec] = Field(None, description="Column selection")
    
    # Pagination for results
    offset: int = Field(0, ge=0, description="Offset for paginated results")
    limit: int = Field(100, ge=1, le=10000, description="Limit for paginated results")


class SamplingJobResponse(BaseModel):
    """Response for sampling job creation."""
    job_id: str
    status: str
    message: str


class SamplingResultResponse(BaseModel):
    """Response for direct sampling."""
    method: str
    sample_size: int
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    strata_counts: Optional[Dict[str, int]] = None
    selected_clusters: Optional[List[Any]] = None


class ColumnSamplesRequest(BaseModel):
    """Request for column value samples."""
    columns: List[str] = Field(..., description="Columns to sample")
    samples_per_column: int = Field(20, ge=1, le=100, description="Samples per column")


class ColumnSamplesResponse(BaseModel):
    """Response for column value samples."""
    samples: Dict[str, List[Any]]
    metadata: Dict[str, Any]


@router.post("/datasets/{dataset_id}/jobs", response_model=SamplingJobResponse)
async def create_sampling_job(
    dataset_id: int = Path(..., description="Dataset ID"),
    request: CreateSamplingJobRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    _: CurrentUser = Depends(require_dataset_read),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service)
) -> SamplingJobResponse:
    """Create a sampling job for asynchronous processing."""
    from ..features.sampling.handlers.create_sampling_job import CreateSamplingJobHandler, CreateSamplingJobCommand
    
    # Convert rounds to handler format
    rounds = []
    for round_config in request.rounds:
        round_params = {
            'method': round_config.method,
            'parameters': round_config.parameters,
            'output_name': round_config.output_name
        }
        
        # Add filters if provided
        if round_config.filters:
            round_params['parameters']['filters'] = round_config.filters.dict()
        
        # Add selection if provided
        if round_config.selection:
            round_params['parameters']['selection'] = round_config.selection.dict()
        
        rounds.append(round_params)
    
    # Create command
    command = CreateSamplingJobCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id,
        source_ref=request.source_ref,
        table_key=request.table_key,
        output_branch_name=request.output_branch_name,  # Can be None
        commit_message=request.commit_message or f"Sampled data from {request.source_ref}",
        rounds=rounds,
        export_residual=request.export_residual,
        residual_output_name=request.residual_output_name
    )
    
    # Create handler and execute
    handler = CreateSamplingJobHandler(uow, permissions=permission_service)
    return await handler.handle(command)


@router.post("/datasets/{dataset_id}/sample", response_model=SamplingResultResponse)
async def sample_data_direct(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: str = Query("main", description="Ref/branch name"),
    table_key: str = Query("primary", description="Table to sample from"),
    request: DirectSamplingRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    _: CurrentUser = Depends(require_dataset_read),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service)
) -> SamplingResultResponse:
    """Perform direct sampling and return results immediately."""
    from ..features.sampling.handlers.sample_data_direct import SampleDataDirectHandler, DirectSamplingCommand
    
    # Create command
    command = DirectSamplingCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id,
        ref_name=ref_name,
        table_key=table_key,
        method=request.method,
        sample_size=request.sample_size,
        random_seed=request.random_seed,
        stratify_columns=request.stratify_columns,
        proportional=request.proportional,
        cluster_column=request.cluster_column,
        num_clusters=request.num_clusters,
        offset=request.offset,
        limit=request.limit
    )
    
    # Create handler and execute
    handler = SampleDataDirectHandler(uow, permissions=permission_service)
    return await handler.handle(command)


@router.post("/datasets/{dataset_id}/column-samples", response_model=ColumnSamplesResponse)
async def get_column_samples(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: str = Query("main", description="Ref/branch name"),
    table_key: str = Query("primary", description="Table to sample from"),
    request: ColumnSamplesRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    _: CurrentUser = Depends(require_dataset_read),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service)
) -> ColumnSamplesResponse:
    """Get unique value samples for specified columns."""
    from ..features.sampling.handlers.get_column_samples import GetColumnSamplesHandler, GetColumnSamplesCommand
    
    # Create command
    command = GetColumnSamplesCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id,
        ref_name=ref_name,
        table_key=table_key,
        columns=request.columns,
        samples_per_column=request.samples_per_column
    )
    
    # Create handler and execute
    handler = GetColumnSamplesHandler(uow, permissions=permission_service)
    return await handler.handle(command)


@router.get("/datasets/{dataset_id}/sampling-methods")
async def get_sampling_methods(
    dataset_id: int = Path(..., description="Dataset ID"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service)
) -> Dict[str, Any]:
    """Get available sampling methods and their parameters."""
    from ..features.sampling.handlers.get_sampling_methods import GetSamplingMethodsHandler, GetSamplingMethodsCommand
    
    # Create command
    command = GetSamplingMethodsCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id
    )
    
    # Create handler and execute
    handler = GetSamplingMethodsHandler(uow, permissions=permission_service)
    return await handler.handle(command)




# New endpoints for sampling job data and history

@router.get("/jobs/{job_id}/data")
async def get_sampling_job_data(
    job_id: str = Path(..., description="Job ID"),
    table_key: str = Query("primary", description="Table to retrieve"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=1000, description="Items per page"),
    columns: Optional[str] = Query(None, description="Comma-separated column names"),
    format: str = Query("json", description="Output format (json or csv)"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    table_reader: PostgresTableReader = Depends(get_table_reader),
    permission_service = Depends(get_permission_service)
):
    """Get sampled data from a completed sampling job."""
    
    # Parse columns if provided
    column_list = columns.split(",") if columns else None
    
    handler = GetSamplingJobDataHandler(uow, table_reader, permissions=permission_service)
    result = await handler.handle(
        job_id=job_id,
        user_id=current_user.user_id,
        table_key=table_key,
        offset=offset,
        limit=limit,
        columns=column_list,
        format=format
    )
    
    # Handle CSV streaming response
    if format == "csv" and result.get('_stream_response'):
        # Create CSV content
        output = io.StringIO()
        
        # Get all data for CSV export (paginate internally if needed)
        all_data = []
        current_offset = 0
        batch_size = 1000
        
        while True:
            batch_result = await handler.handle(
                job_id=job_id,
                user_id=current_user.user_id,
                table_key=table_key,
                offset=current_offset,
                limit=batch_size,
                columns=column_list,
                format="json"  # Get JSON internally
            )
            
            all_data.extend(batch_result['data'])
            current_offset += batch_size
            
            if len(batch_result['data']) < batch_size:
                break
        
        # Write CSV
        if all_data:
            writer = csv.DictWriter(output, fieldnames=all_data[0].keys())
            writer.writeheader()
            writer.writerows(all_data)
        
        # Return streaming response
        output.seek(0)
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={result['filename']}"
            }
        )
    
    return result


@router.get("/jobs/{job_id}/residual")
async def get_sampling_job_residual(
    job_id: str = Path(..., description="Job ID"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=1000, description="Items per page"),
    columns: Optional[str] = Query(None, description="Comma-separated column names"),
    format: str = Query("json", description="Output format (json or csv)"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    table_reader: PostgresTableReader = Depends(get_table_reader),
    permission_service = Depends(get_permission_service)
):
    """Get residual (unsampled) data from a sampling job."""
    
    # This is just a wrapper that calls the main endpoint with table_key="residual"
    return await get_sampling_job_data(
        job_id=job_id,
        table_key="residual",
        offset=offset,
        limit=limit,
        columns=columns,
        format=format,
        current_user=current_user,
        uow=uow,
        table_reader=table_reader
    )


@router.get("/datasets/{dataset_id}/history")
async def get_dataset_sampling_history(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: Optional[str] = Query(None, description="Filter by ref name"),
    status: Optional[str] = Query(None, description="Filter by job status"),
    start_date: Optional[datetime] = Query(None, description="Filter jobs created after"),
    end_date: Optional[datetime] = Query(None, description="Filter jobs created before"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service)
):
    """Get sampling job history for a dataset."""
    
    handler = GetDatasetSamplingHistoryHandler(uow, permissions=permission_service)
    return await handler.handle(
        dataset_id=dataset_id,
        user_id=current_user.user_id,
        ref_name=ref_name,
        status=status,
        start_date=start_date,
        end_date=end_date,
        offset=offset,
        limit=limit
    )


@router.get("/users/{user_id}/history")
async def get_user_sampling_history(
    user_id: int = Path(..., description="User ID"),
    dataset_id: Optional[int] = Query(None, description="Filter by dataset"),
    status: Optional[str] = Query(None, description="Filter by job status"),
    start_date: Optional[datetime] = Query(None, description="Filter jobs created after"),
    end_date: Optional[datetime] = Query(None, description="Filter jobs created before"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    permission_service = Depends(get_permission_service)
):
    """Get sampling job history for a user."""
    
    handler = GetUserSamplingHistoryHandler(uow, permissions=permission_service)
    return await handler.handle(
        target_user_id=user_id,
        current_user_id=current_user.user_id,
        is_admin=current_user.is_admin(),
        dataset_id=dataset_id,
        status=status,
        start_date=start_date,
        end_date=end_date,
        offset=offset,
        limit=limit
    )