from enum import Enum
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field, validator
from datetime import datetime
import uuid

class SamplingMethod(str, Enum):
    """Available sampling methods"""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    CLUSTER = "cluster"
    CUSTOM = "custom"
    WEIGHTED = "weighted"

class JobStatus(str, Enum):
    """Status of a sampling job"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

# Request models for different sampling methods
class RandomSamplingParams(BaseModel):
    """Parameters for random sampling"""
    sample_size: int = Field(..., description="Number of samples to draw")
    seed: Optional[int] = Field(None, description="Random seed for reproducibility")

class StratifiedSamplingParams(BaseModel):
    """Parameters for stratified sampling"""
    strata_columns: List[str] = Field(..., description="Columns to stratify by")
    sample_size: Optional[Union[int, float]] = Field(None, description="Number or fraction of samples to draw")
    min_per_stratum: Optional[int] = Field(None, description="Minimum samples per stratum")
    seed: Optional[int] = Field(None, description="Random seed for reproducibility")
    
    @validator('sample_size')
    def validate_sample_size(cls, v):
        if v is not None and isinstance(v, float) and (v <= 0 or v >= 1):
            raise ValueError("If sample_size is a fraction, it must be between 0 and 1")
        return v

class SystematicSamplingParams(BaseModel):
    """Parameters for systematic sampling"""
    interval: int = Field(..., description="Take every nth record")
    start: Optional[int] = Field(0, description="Starting index (default is 0)")

class ClusterSamplingParams(BaseModel):
    """Parameters for cluster sampling"""
    cluster_column: str = Field(..., description="Column that identifies clusters")
    num_clusters: int = Field(..., description="Number of clusters to sample")
    sample_within_clusters: Optional[bool] = Field(False, description="Whether to sample within selected clusters")

class CustomSamplingParams(BaseModel):
    """Parameters for custom sampling"""
    query: str = Field(..., description="SQL WHERE clause for filtering")

class WeightedSamplingParams(BaseModel):
    """Parameters for weighted sampling"""
    weight_column: str = Field(..., description="Column containing sampling weights")
    sample_size: int = Field(..., description="Number of samples to draw")
    seed: Optional[int] = Field(None, description="Random seed for reproducibility")

# New filtering models
class FilterCondition(BaseModel):
    """A single filter condition"""
    column: str = Field(..., description="Column name to filter on")
    operator: str = Field(..., description="Operator: =, !=, >, <, >=, <=, LIKE, IN, NOT IN")
    value: Union[str, int, float, List[Union[str, int, float]]] = Field(..., description="Value(s) to compare against")
    
    @validator('operator')
    def validate_operator(cls, v):
        allowed_ops = ['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'ILIKE', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL']
        if v not in allowed_ops:
            raise ValueError(f"Operator must be one of: {allowed_ops}")
        return v

class DataFilters(BaseModel):
    """Data filtering options - supports nested filter groups"""
    conditions: Optional[List[FilterCondition]] = Field(None, description="Filter conditions")
    groups: Optional[List['DataFilters']] = Field(None, description="Nested filter groups")
    logic: str = Field("AND", description="Logic between conditions and groups: AND or OR")
    
    @validator('logic')
    def validate_logic(cls, v):
        if v not in ['AND', 'OR']:
            raise ValueError("Logic must be 'AND' or 'OR'")
        return v
    
    @validator('groups')
    def validate_has_content(cls, v, values):
        conditions = values.get('conditions')
        if not conditions and not v:
            raise ValueError("DataFilters must have either conditions or groups")
        return v

# Enable forward reference for recursive model
DataFilters.model_rebuild()

class DataSelection(BaseModel):
    """Data selection and ordering options"""
    columns: Optional[List[str]] = Field(None, description="Columns to select (null = all)")
    exclude_columns: Optional[List[str]] = Field(None, description="Columns to exclude")
    order_by: Optional[str] = Field(None, description="Column to order by")
    order_desc: bool = Field(False, description="Use descending order")
    limit: Optional[int] = Field(None, description="Limit number of rows before sampling")
    offset: Optional[int] = Field(None, description="Skip number of rows before sampling")

# Main request model
class SamplingRequest(BaseModel):
    """
    Request model for the sampling endpoint.
    Contains options for dataset sampling.
    """
    sheet: Optional[str] = Field(
        None, 
        description="Optional sheet name for Excel files"
    )
    method: SamplingMethod = Field(
        ...,
        description="Sampling method to use"
    )
    parameters: Dict[str, Any] = Field(
        ...,
        description="Parameters specific to the sampling method"
    )
    output_name: str = Field(
        ...,
        description="Name for the output sample dataset"
    )
    # New filtering and selection options
    filters: Optional[DataFilters] = Field(
        None,
        description="Row filtering options"
    )
    selection: Optional[DataSelection] = Field(
        None,
        description="Column selection and ordering options"
    )

    def get_typed_parameters(self):
        """Convert parameters dict to the appropriate typed model"""
        if self.method == SamplingMethod.RANDOM:
            return RandomSamplingParams(**self.parameters)
        elif self.method == SamplingMethod.STRATIFIED:
            return StratifiedSamplingParams(**self.parameters)
        elif self.method == SamplingMethod.SYSTEMATIC:
            return SystematicSamplingParams(**self.parameters)
        elif self.method == SamplingMethod.CLUSTER:
            return ClusterSamplingParams(**self.parameters)
        elif self.method == SamplingMethod.WEIGHTED:
            return WeightedSamplingParams(**self.parameters)
        elif self.method == SamplingMethod.CUSTOM:
            return CustomSamplingParams(**self.parameters)
        else:
            raise ValueError(f"Unknown sampling method: {self.method}")

# Response models
class SamplingJobResponse(BaseModel):
    """Base response for sampling job operations"""
    run_id: str = Field(..., description="Unique identifier for the sampling job")
    status: JobStatus = Field(..., description="Current status of the job")
    message: str = Field(..., description="Human-readable message about the job")

class DataSummary(BaseModel):
    """Data summary statistics"""
    total_rows: int
    total_columns: int
    column_types: Dict[str, str]
    memory_usage_mb: float
    null_counts: Dict[str, int]

class SamplingJobDetails(SamplingJobResponse):
    """Detailed information about a sampling job"""
    started_at: Optional[datetime] = Field(None, description="When the job started")
    completed_at: Optional[datetime] = Field(None, description="When the job completed")
    output_preview: Optional[List[Dict[str, Any]]] = Field(None, description="Preview of sampled data")
    output_uri: Optional[str] = Field(None, description="URI of the complete output file")
    error_message: Optional[str] = Field(None, description="Error message if job failed")
    data_summary: Optional[DataSummary] = Field(None, description="Summary of the original data")
    sample_summary: Optional[DataSummary] = Field(None, description="Summary of the sampled data")

# Database model for storing job information
class SamplingJob(BaseModel):
    """Internal model for tracking sampling jobs"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    dataset_id: int
    version_id: int
    user_id: int
    request: SamplingRequest
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    output_preview: Optional[List[Dict[str, Any]]] = None
    output_uri: Optional[str] = None
    error_message: Optional[str] = None
    data_summary: Optional[DataSummary] = None
    sample_summary: Optional[DataSummary] = None

# Multi-round sampling models
class SamplingRoundConfig(BaseModel):
    """Configuration for a single sampling round"""
    round_number: int = Field(..., description="Round number (1-based)")
    method: SamplingMethod = Field(..., description="Sampling method for this round")
    parameters: Dict[str, Any] = Field(..., description="Parameters for the sampling method")
    output_name: str = Field(..., description="Name for this round's output")
    filters: Optional[DataFilters] = Field(None, description="Additional filters for this round")
    selection: Optional[DataSelection] = Field(None, description="Column selection for this round")
    
    def get_typed_parameters(self):
        """Convert parameters dict to the appropriate typed model"""
        if self.method == SamplingMethod.RANDOM:
            return RandomSamplingParams(**self.parameters)
        elif self.method == SamplingMethod.STRATIFIED:
            return StratifiedSamplingParams(**self.parameters)
        elif self.method == SamplingMethod.SYSTEMATIC:
            return SystematicSamplingParams(**self.parameters)
        elif self.method == SamplingMethod.CLUSTER:
            return ClusterSamplingParams(**self.parameters)
        elif self.method == SamplingMethod.WEIGHTED:
            return WeightedSamplingParams(**self.parameters)
        elif self.method == SamplingMethod.CUSTOM:
            return CustomSamplingParams(**self.parameters)
        else:
            raise ValueError(f"Unknown sampling method: {self.method}")

class MultiRoundSamplingRequest(BaseModel):
    """Request for multi-round progressive sampling"""
    rounds: List[SamplingRoundConfig] = Field(..., description="Configuration for each sampling round")
    export_residual: bool = Field(True, description="Whether to export the final residual dataset")
    residual_output_name: Optional[str] = Field("residual", description="Name for the residual dataset")
    sheet: Optional[str] = Field(None, description="Optional sheet name for Excel files")
    
    @validator('rounds')
    def validate_rounds(cls, v):
        if not v:
            raise ValueError("At least one sampling round must be specified")
        round_numbers = [r.round_number for r in v]
        if sorted(round_numbers) != list(range(1, len(v) + 1)):
            raise ValueError("Round numbers must be sequential starting from 1")
        return v

class RoundResult(BaseModel):
    """Result of a single sampling round"""
    round_number: int
    method: SamplingMethod
    sample_size: int
    output_uri: str
    preview: Optional[List[Dict[str, Any]]] = None
    summary: Optional[DataSummary] = None
    started_at: datetime
    completed_at: datetime

class MultiRoundSamplingJobResponse(BaseModel):
    """Response for multi-round sampling job status"""
    run_id: str = Field(..., description="Unique identifier for the multi-round job")
    status: JobStatus = Field(..., description="Overall status of the job")
    message: str = Field(..., description="Human-readable message")
    total_rounds: int = Field(..., description="Total number of rounds")
    completed_rounds: int = Field(..., description="Number of completed rounds")
    current_round: Optional[int] = Field(None, description="Currently executing round")
    round_results: List[RoundResult] = Field(default_factory=list, description="Results from completed rounds")
    residual_uri: Optional[str] = Field(None, description="URI of the final residual dataset")
    residual_size: Optional[int] = Field(None, description="Number of records in residual")
    residual_summary: Optional[DataSummary] = Field(None, description="Summary of residual dataset")
    error_message: Optional[str] = Field(None, description="Error message if job failed")
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class MultiRoundSamplingJob(BaseModel):
    """Internal model for tracking multi-round sampling jobs"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    dataset_id: int
    version_id: int
    user_id: int
    request: MultiRoundSamplingRequest
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_rounds: int
    completed_rounds: int = 0
    current_round: Optional[int] = None
    round_results: List[RoundResult] = Field(default_factory=list)
    residual_uri: Optional[str] = None
    residual_size: Optional[int] = None
    residual_summary: Optional[DataSummary] = None
    error_message: Optional[str] = None
    # Track residual dataset path internally
    residual_table_name: Optional[str] = None