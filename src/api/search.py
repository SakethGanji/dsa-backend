"""API routes for search functionality."""

from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional, List
from datetime import datetime

from ..core.database import DatabasePool, UnitOfWorkFactory
from ..core.authorization import get_current_user_info
from ..features.search.handlers import SearchDatasetsHandler, SuggestHandler
from ..features.search.models import (
    SearchRequest, 
    SuggestRequest, 
    SearchResponse, 
    SuggestResponse
)
from ..models.pydantic_models import CurrentUser
from ..core.abstractions.uow import IUnitOfWork


router = APIRouter(prefix="/datasets/search", tags=["search"])


# Dependency injection helpers (will be overridden in main.py)
def get_db_pool() -> DatabasePool:
    """Get database pool."""
    raise NotImplementedError("Database pool not configured")


async def get_uow_factory(
    pool: DatabasePool = Depends(get_db_pool)
) -> UnitOfWorkFactory:
    """Get unit of work factory."""
    return UnitOfWorkFactory(pool)


@router.get("/", response_model=SearchResponse, summary="Search datasets")
async def search_datasets(
    # Query parameters
    query: Optional[str] = Query(
        None, 
        description="Main search query. Supports keywords like 'tag:value' and 'user:value'"
    ),
    fuzzy: bool = Query(
        True, 
        description="Use fuzzy/typo-tolerant search"
    ),
    
    # Filter parameters
    tags: Optional[List[str]] = Query(
        None, 
        description="Filter by datasets containing ALL of these tags"
    ),
    created_by: Optional[List[int]] = Query(
        None, 
        description="Filter by creator user IDs"
    ),
    created_after: Optional[datetime] = Query(
        None, 
        description="Filter by creation date after this time"
    ),
    created_before: Optional[datetime] = Query(
        None, 
        description="Filter by creation date before this time"
    ),
    updated_after: Optional[datetime] = Query(
        None, 
        description="Filter by update date after this time"
    ),
    updated_before: Optional[datetime] = Query(
        None, 
        description="Filter by update date before this time"
    ),
    
    # Pagination parameters
    limit: int = Query(
        20, 
        ge=1, 
        le=100, 
        description="Number of results to return"
    ),
    offset: int = Query(
        0, 
        ge=0, 
        description="Number of results to skip"
    ),
    
    # Sorting parameters
    sort_by: str = Query(
        'relevance', 
        description="Sort field: relevance, name, created_at, updated_at"
    ),
    sort_order: str = Query(
        'desc', 
        description="Sort order: asc or desc"
    ),
    
    # Faceting parameters
    include_facets: bool = Query(
        True, 
        description="Include facet counts in response"
    ),
    facet_fields: Optional[List[str]] = Query(
        None,
        description="Which facets to calculate: tags, created_by"
    ),
    
    # Dependencies
    current_user: CurrentUser = Depends(get_current_user_info),
    pool: DatabasePool = Depends(get_db_pool)
):
    """
    Search for datasets with advanced filtering and faceting.
    
    The search supports:
    - Full-text search with fuzzy matching
    - Filtering by tags, creator, and date ranges
    - Sorting by relevance, name, or timestamps
    - Faceted search for discovering filter options
    - Special query syntax: 'tag:finance', 'user:jsmith'
    
    Returns paginated results with relevance scores and facet counts.
    """
    # Validate sort_by parameter
    valid_sort_by = ['relevance', 'name', 'created_at', 'updated_at']
    if sort_by not in valid_sort_by:
        raise HTTPException(400, f"Invalid sort_by. Must be one of: {', '.join(valid_sort_by)}")
    
    # Validate sort_order parameter
    if sort_order not in ['asc', 'desc']:
        raise HTTPException(400, "Invalid sort_order. Must be 'asc' or 'desc'")
    
    # Default facet fields if not specified
    if facet_fields is None:
        facet_fields = ['tags', 'created_by']
    
    # Validate facet fields
    valid_facet_fields = ['tags', 'created_by']
    for field in facet_fields:
        if field not in valid_facet_fields:
            raise HTTPException(400, f"Invalid facet field: {field}. Must be one of: {', '.join(valid_facet_fields)}")
    
    # Create UnitOfWork and handler
    async with pool.acquire() as conn:
        from ..core.infrastructure.postgres.uow import PostgresUnitOfWork
        uow = PostgresUnitOfWork(pool)
        handler = SearchDatasetsHandler(uow)
        
        request = SearchRequest(
            context={'user_id': current_user.user_id},
            query=query,
            fuzzy=fuzzy,
            tags=tags,
            created_by=created_by,
            created_after=created_after,
            created_before=created_before,
            updated_after=updated_after,
            updated_before=updated_before,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order,
            include_facets=include_facets,
            facet_fields=facet_fields
        )
        
        return await handler.handle(request)


@router.get("/suggest", response_model=SuggestResponse, summary="Get search suggestions")
async def suggest(
    query: str = Query(
        ..., 
        min_length=1, 
        description="Partial text to get suggestions for"
    ),
    limit: int = Query(
        10, 
        ge=1, 
        le=50, 
        description="Maximum number of suggestions"
    ),
    current_user: CurrentUser = Depends(get_current_user_info),
    pool: DatabasePool = Depends(get_db_pool)
):
    """
    Get autocomplete suggestions for dataset names and tags.
    
    Returns suggestions based on:
    - Dataset names that match the partial query
    - Tag names that match the partial query
    
    Suggestions are sorted by relevance and filtered by user permissions.
    """
    # Create UnitOfWork and handler
    async with pool.acquire() as conn:
        from ..core.infrastructure.postgres.uow import PostgresUnitOfWork
        uow = PostgresUnitOfWork(pool)
        handler = SuggestHandler(uow)
        
        request = SuggestRequest(
            context={'user_id': current_user.user_id},
            query=query,
            limit=limit
        )
        
        return await handler.handle(request)