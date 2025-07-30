"""Response models for search functionality."""

from pydantic import BaseModel
from typing import List, Optional, Dict, Literal
from datetime import datetime


class SearchResult(BaseModel):
    """Individual search result for a dataset."""
    
    id: int
    name: str
    description: Optional[str]
    created_by: int
    created_by_name: Optional[str]
    created_at: datetime
    updated_at: datetime
    tags: List[str]
    score: Optional[float]  # Relevance score (0-1), only present when sorting by relevance
    user_permission: Literal['read', 'write', 'admin']
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SearchFacets(BaseModel):
    """Facet counts for search results."""
    
    tags: Optional[Dict[str, int]] = None
    created_by: Optional[Dict[str, int]] = None


class SearchResponse(BaseModel):
    """Response model for dataset search."""
    
    results: List[SearchResult]
    total: int
    limit: int
    offset: int
    has_more: bool
    query: Optional[str]
    execution_time_ms: int
    facets: Optional[SearchFacets]
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "results": [
                    {
                        "id": 123,
                        "name": "Q4 Financial Report 2024",
                        "description": "Quarterly financial data for Q4 2024",
                        "created_by": 456,
                        "created_by_name": "jsmith",
                        "created_at": "2024-01-15T10:30:00Z",
                        "updated_at": "2024-01-20T14:45:00Z",
                        "tags": ["finance", "quarterly", "2024"],
                        "score": 0.85,
                        "user_permission": "read"
                    }
                ],
                "total": 42,
                "limit": 20,
                "offset": 0,
                "has_more": True,
                "query": "financial report",
                "execution_time_ms": 23,
                "facets": {
                    "tags": {
                        "finance": 25,
                        "quarterly": 18,
                        "2024": 15
                    },
                    "created_by": {
                        "jsmith": 12,
                        "adoe": 8
                    }
                }
            }
        }


class Suggestion(BaseModel):
    """Individual autocomplete suggestion."""
    
    text: str
    type: Literal['dataset_name', 'tag']
    score: float


class SuggestResponse(BaseModel):
    """Response model for autocomplete suggestions."""
    
    suggestions: List[Suggestion]
    query: str
    execution_time_ms: int
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "suggestions": [
                    {
                        "text": "Financial Report Q4 2024",
                        "type": "dataset_name",
                        "score": 0.92
                    },
                    {
                        "text": "finance",
                        "type": "tag",
                        "score": 0.88
                    }
                ],
                "query": "finan",
                "execution_time_ms": 5
            }
        }