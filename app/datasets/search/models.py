"""Models for dataset search functionality"""
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from enum import Enum


class SearchSortBy(str, Enum):
    """Available sort options for search results"""
    RELEVANCE = "relevance"
    NAME = "name"
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"
    FILE_SIZE = "file_size"
    VERSION_COUNT = "version_count"


class SearchFilter(BaseModel):
    """Individual search filter"""
    field: str
    operator: str  # eq, ne, gt, lt, gte, lte, in, like, ilike
    value: Union[str, int, float, List[Any], datetime]


class DateRangeFilter(BaseModel):
    """Date range filter for search"""
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class NumericRangeFilter(BaseModel):
    """Numeric range filter for search"""
    min: Optional[Union[int, float]] = None
    max: Optional[Union[int, float]] = None


class SearchRequest(BaseModel):
    """Request model for dataset search"""
    # Main search query
    query: Optional[str] = Field(None, description="Search query string")
    
    # Filters
    tags: Optional[List[str]] = Field(None, description="Filter by tags")
    file_types: Optional[List[str]] = Field(None, description="Filter by file types")
    created_by: Optional[List[int]] = Field(None, description="Filter by creator user IDs")
    
    # Range filters
    created_at: Optional[DateRangeFilter] = Field(None, description="Filter by creation date range")
    updated_at: Optional[DateRangeFilter] = Field(None, description="Filter by update date range")
    file_size: Optional[NumericRangeFilter] = Field(None, description="Filter by file size range")
    version_count: Optional[NumericRangeFilter] = Field(None, description="Filter by number of versions")
    
    # Advanced filters
    has_schema: Optional[bool] = Field(None, description="Filter datasets with/without schema")
    has_permissions: Optional[bool] = Field(None, description="Filter datasets with custom permissions")
    
    # Search options
    search_in_description: bool = Field(True, description="Include description in search")
    search_in_tags: bool = Field(True, description="Include tags in search")
    fuzzy_search: bool = Field(False, description="Enable fuzzy/typo-tolerant search")
    
    # Pagination and sorting
    limit: int = Field(20, ge=1, le=100, description="Number of results per page")
    offset: int = Field(0, ge=0, description="Number of results to skip")
    sort_by: SearchSortBy = Field(SearchSortBy.RELEVANCE, description="Sort field")
    sort_order: str = Field("desc", pattern="^(asc|desc)$", description="Sort order")
    
    # Faceting
    include_facets: bool = Field(True, description="Include facet counts in response")
    facet_fields: Optional[List[str]] = Field(
        None, 
        description="Fields to compute facets for. If None, defaults to common fields"
    )


class SearchResult(BaseModel):
    """Individual search result"""
    # Dataset info
    id: int
    name: str
    description: Optional[str] = None
    created_by: int
    created_by_name: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    
    # Version and file info
    current_version: Optional[int] = None
    version_count: int = 0
    file_type: Optional[str] = None
    file_size: Optional[int] = None
    
    # Tags
    tags: List[str] = []
    
    # Search metadata
    score: float = Field(..., description="Relevance score (0-1)")
    highlights: Optional[Dict[str, List[str]]] = Field(
        None, 
        description="Highlighted snippets from matched fields"
    )
    
    # Permissions
    user_permission: Optional[str] = Field(None, description="Current user's permission level")


class FacetValue(BaseModel):
    """Individual facet value with count"""
    value: Union[str, int, float]
    label: Optional[str] = None
    count: int


class SearchFacet(BaseModel):
    """Facet information for a field"""
    field: str
    label: str
    values: List[FacetValue]
    total_values: int


class SearchFacets(BaseModel):
    """Collection of search facets"""
    tags: Optional[SearchFacet] = None
    file_types: Optional[SearchFacet] = None
    created_by: Optional[SearchFacet] = None
    years: Optional[SearchFacet] = None


class SearchSuggestion(BaseModel):
    """Search suggestion/autocomplete result"""
    text: str
    type: str = Field(..., description="Type of suggestion: dataset_name, tag, user")
    score: float = Field(..., description="Suggestion score")
    metadata: Optional[Dict[str, Any]] = None


class SearchResponse(BaseModel):
    """Response model for dataset search"""
    # Results
    results: List[SearchResult]
    total: int = Field(..., description="Total number of matching datasets")
    
    # Pagination info
    limit: int
    offset: int
    has_more: bool
    
    # Search metadata
    query: Optional[str] = None
    execution_time_ms: float = Field(..., description="Search execution time in milliseconds")
    
    # Facets
    facets: Optional[SearchFacets] = None
    
    # Suggestions (for "did you mean" functionality)
    suggestions: Optional[List[str]] = None


class SearchSuggestRequest(BaseModel):
    """Request model for search suggestions/autocomplete"""
    query: str = Field(..., min_length=1, description="Partial search query")
    limit: int = Field(10, ge=1, le=50, description="Maximum number of suggestions")
    types: Optional[List[str]] = Field(
        None, 
        description="Types of suggestions to include (dataset_name, tag, user)"
    )


class SearchSuggestResponse(BaseModel):
    """Response model for search suggestions"""
    suggestions: List[SearchSuggestion]
    query: str
    execution_time_ms: float