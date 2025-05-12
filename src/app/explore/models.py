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
    Contains operations to apply to the dataset before profiling.
    """
    operations: List[Dict[str, Any]]
    sheet: Optional[str] = None  # Optional sheet name for Excel files
    format: ProfileFormat = ProfileFormat.JSON  # Default to JSON format
    run_profiling: bool = False  # Default to False to avoid timeout issues with large datasets
    
class ProfileResponse(BaseModel):
    """
    Response model for the explore endpoint.
    Contains the profiling report.
    """
    profile: Dict[str, Any]