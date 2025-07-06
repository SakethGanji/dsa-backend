"""Request models for search functionality."""

from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime


class SearchRequest(BaseModel):
    """Request model for dataset search."""
    
    # Context from authentication
    context: dict  # Contains user_id from auth
    
    # Core search parameters
    query: Optional[str] = None
    fuzzy: bool = True
    
    # Filter parameters
    tags: Optional[List[str]] = None
    created_by: Optional[List[int]] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    updated_after: Optional[datetime] = None
    updated_before: Optional[datetime] = None
    
    # Pagination parameters
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)
    
    # Sorting parameters
    sort_by: Literal['relevance', 'name', 'created_at', 'updated_at'] = 'relevance'
    sort_order: Literal['asc', 'desc'] = 'desc'
    
    # Faceting parameters
    include_facets: bool = True
    facet_fields: Optional[List[Literal['tags', 'created_by']]] = None
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "query": "financial report tag:quarterly",
                "fuzzy": True,
                "tags": ["finance", "2024"],
                "limit": 20,
                "offset": 0,
                "sort_by": "relevance",
                "sort_order": "desc",
                "include_facets": True,
                "facet_fields": ["tags", "created_by"]
            }
        }


class SuggestRequest(BaseModel):
    """Request model for autocomplete suggestions."""
    
    # Context from authentication
    context: dict  # Contains user_id from auth
    
    # Query parameters
    query: str = Field(..., min_length=1, description="Partial text to get suggestions for")
    limit: int = Field(default=10, ge=1, le=50)
    
    class Config:
        """Pydantic configuration."""
        schema_extra = {
            "example": {
                "query": "finan",
                "limit": 10
            }
        }