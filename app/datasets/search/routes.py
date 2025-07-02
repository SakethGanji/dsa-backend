"""Routes for dataset search API"""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import List, Optional, Annotated
from sqlalchemy.ext.asyncio import AsyncSession

from app.datasets.search.models import (
    SearchRequest, SearchResponse, SearchSuggestRequest, SearchSuggestResponse
)
from app.datasets.search.service import SearchService
from app.datasets.search.repository import SearchRepository
from app.db.connection import get_session
from app.users.auth import get_current_user_info, CurrentUser
from app.users.service import UserService

import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["Dataset Search"])

# Dependency injection
async def get_search_service(session: AsyncSession = Depends(get_session)) -> SearchService:
    """Get search service instance"""
    search_repository = SearchRepository(session)
    user_service = UserService(session)
    return SearchService(search_repository, user_service)

# Type aliases
SearchServiceDep = Annotated[SearchService, Depends(get_search_service)]
UserDep = Annotated[CurrentUser, Depends(get_current_user_info)]




@router.get(
    "",
    response_model=SearchResponse,
    summary="Search datasets",
    description="Search datasets with full-text search, filtering, and faceting"
)
async def search_datasets_get(
    # Basic search parameters
    query: Optional[str] = Query(None, description="Search query string"),
    
    # Filter parameters
    tags: Optional[List[str]] = Query(None, description="Filter by tags (AND logic)"),
    file_types: Optional[List[str]] = Query(None, description="Filter by file types"),
    created_by: Optional[List[int]] = Query(None, description="Filter by creator user IDs"),
    
    # Date range filters (ISO format: 2024-01-01T00:00:00Z)
    created_after: Optional[str] = Query(None, description="Filter datasets created after this date"),
    created_before: Optional[str] = Query(None, description="Filter datasets created before this date"),
    updated_after: Optional[str] = Query(None, description="Filter datasets updated after this date"),
    updated_before: Optional[str] = Query(None, description="Filter datasets updated before this date"),
    
    # Numeric filters
    size_min: Optional[int] = Query(None, ge=0, description="Minimum file size in bytes"),
    size_max: Optional[int] = Query(None, ge=0, description="Maximum file size in bytes"),
    versions_min: Optional[int] = Query(None, ge=1, description="Minimum number of versions"),
    versions_max: Optional[int] = Query(None, ge=1, description="Maximum number of versions"),
    
    # Search options
    fuzzy: bool = Query(False, description="Enable fuzzy/typo-tolerant search"),
    search_description: bool = Query(True, description="Include description in search"),
    search_tags: bool = Query(True, description="Include tags in search"),
    
    # Pagination and sorting
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    sort_by: str = Query("relevance", description="Sort field (relevance|name|created_at|updated_at|file_size|version_count)"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$", description="Sort order"),
    
    # Response options
    include_facets: bool = Query(True, description="Include facet counts for filtering"),
    facet_fields: Optional[List[str]] = Query(None, description="Specific facet fields to include (default: tags, file_types)"),
    service: SearchServiceDep = None,
    current_user: UserDep = None
) -> SearchResponse:
    """
    Search datasets with advanced filtering and full-text search.
    
    Features:
    - Full-text search across dataset names and descriptions
    - Fuzzy/typo-tolerant search option
    - Advanced filtering by tags, file types, dates, sizes, etc.
    - Faceted search results for dynamic filtering
    - Permission-aware results (only shows datasets user has access to)
    - Relevance-based ranking when searching with query
    - Support for complex date and numeric range filters
    
    Examples:
    - Simple search: /api/datasets/search?query=financial+report
    - Fuzzy search: /api/datasets/search?query=finacial&fuzzy=true
    - Tag filter: /api/datasets/search?tags=finance&tags=quarterly
    - Date filter: /api/datasets/search?created_after=2024-01-01T00:00:00Z
    - Size filter: /api/datasets/search?size_min=1048576&size_max=104857600
    - Combined: /api/datasets/search?query=sales&tags=2024&file_types=csv&fuzzy=true
    """
    # Parse date filters
    from datetime import datetime
    from app.datasets.search.models import DateRangeFilter, NumericRangeFilter
    
    created_at_filter = None
    if created_after or created_before:
        try:
            created_at_filter = DateRangeFilter(
                start=datetime.fromisoformat(created_after.replace('Z', '+00:00')) if created_after else None,
                end=datetime.fromisoformat(created_before.replace('Z', '+00:00')) if created_before else None
            )
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Use ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ"
            )
    
    updated_at_filter = None
    if updated_after or updated_before:
        try:
            updated_at_filter = DateRangeFilter(
                start=datetime.fromisoformat(updated_after.replace('Z', '+00:00')) if updated_after else None,
                end=datetime.fromisoformat(updated_before.replace('Z', '+00:00')) if updated_before else None
            )
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Use ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ"
            )
    
    # Parse numeric filters
    file_size_filter = None
    if size_min is not None or size_max is not None:
        file_size_filter = NumericRangeFilter(min=size_min, max=size_max)
    
    version_count_filter = None
    if versions_min is not None or versions_max is not None:
        version_count_filter = NumericRangeFilter(min=versions_min, max=versions_max)
    
    # Convert to SearchRequest with all parameters
    request = SearchRequest(
        query=query,
        tags=tags,
        file_types=file_types,
        created_by=created_by,
        created_at=created_at_filter,
        updated_at=updated_at_filter,
        file_size=file_size_filter,
        version_count=version_count_filter,
        fuzzy_search=fuzzy,
        search_in_description=search_description,
        search_in_tags=search_tags,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order,
        include_facets=include_facets,
        facet_fields=facet_fields
    )
    
    try:
        # Validate request
        service.validate_search_request(request)
        
        # Perform search
        response = await service.search_datasets(request, current_user)
        
        return response
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Search error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred during search: {str(e)}"
        )




@router.get(
    "/suggest",
    response_model=SearchSuggestResponse,
    summary="Get search suggestions",
    description="Get autocomplete suggestions for search queries"
)
async def get_search_suggestions_get(
    query: str = Query(..., min_length=1, description="Partial search query"),
    limit: int = Query(10, ge=1, le=50, description="Maximum suggestions"),
    types: Optional[List[str]] = Query(None, description="Types to include"),
    service: SearchServiceDep = None,
    current_user: UserDep = None
) -> SearchSuggestResponse:
    """
    Get search suggestions/autocomplete results based on partial query.
    
    Returns suggestions from:
    - Dataset names matching the query
    - Tag names matching the query
    - Sorted by relevance score
    
    Use this for implementing search-as-you-type functionality.
    """
    request = SearchSuggestRequest(
        query=query,
        limit=limit,
        types=types
    )
    
    try:
        response = await service.get_suggestions(request)
        return response
    except Exception as e:
        logger.error(f"Suggestion error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred getting suggestions"
        )


