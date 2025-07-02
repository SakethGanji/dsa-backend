from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

class ProfileFormat(str, Enum):
    """Format options for the profile output"""
    JSON = "json"
    HTML = "html"

class ExploreRequest(BaseModel):
    """
    Request model for the explore endpoint.
    Contains options for dataset loading and profiling.
    """
    sheet: Optional[str] = Field(
        None, 
        description="Optional sheet name for Excel files"
    )
    format: ProfileFormat = Field(
        ProfileFormat.JSON,
        description="Output format for the profile report (json or html)"
    )
    run_profiling: bool = Field(
        True,
        description="Whether to run full profiling (may be slow for large datasets)"
    )
    sample_size: Optional[int] = Field(
        None,
        description="Number of rows to sample for profiling. If not specified, auto-sampling will be used for large datasets"
    )
    sampling_method: str = Field(
        "random",
        description="Sampling method to use: random, systematic, or stratified"
    )
    auto_sample_threshold: int = Field(
        50000,
        description="Automatically sample datasets larger than this number of rows"
    )
    
class ProfileResponse(BaseModel):
    """
    Response model for the explore endpoint.
    Contains the profiling report.
    """
    format: str = Field(..., description="Format of the profile (json or html)")
    summary: Dict[str, Any] = Field(..., description="Summary of the dataset")
    profile: Optional[Dict[str, Any]] = Field(None, description="Profiling report (if run_profiling is True)")
    sampling_info: Optional[Dict[str, Any]] = Field(None, description="Information about sampling applied to the dataset")

