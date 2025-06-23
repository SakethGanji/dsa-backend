"""Routes for dataset search API"""
from fastapi import APIRouter, Depends, HTTPException, Query, Body, status
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

router = APIRouter(prefix="/api/datasets/search", tags=["Dataset Search"])

# Dependency injection
async def get_search_service(session: AsyncSession = Depends(get_session)) -> SearchService:
    """Get search service instance"""
    search_repository = SearchRepository(session)
    user_service = UserService(session)
    return SearchService(search_repository, user_service)

# Type aliases
SearchServiceDep = Annotated[SearchService, Depends(get_search_service)]
UserDep = Annotated[CurrentUser, Depends(get_current_user_info)]


@router.post(
    "",
    response_model=SearchResponse,
    summary="Search datasets",
    description="Search datasets with full-text search, filtering, and faceting"
)
async def search_datasets(
    request: SearchRequest = Body(..., description="Search parameters"),
    service: SearchServiceDep = None,
    current_user: UserDep = None
) -> SearchResponse:
    """
    Search datasets with advanced filtering and full-text search.
    
    Features:
    - Full-text search across dataset names and descriptions
    - Fuzzy/typo-tolerant search option
    - Advanced filtering by tags, file types, dates, sizes, etc.
    - Faceted search results
    - Permission-aware results (only shows datasets user has access to)
    - Relevance-based ranking
    """
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
            detail="An error occurred during search"
        )


@router.get(
    "",
    response_model=SearchResponse,
    summary="Search datasets (GET)",
    description="Search datasets using query parameters"
)
async def search_datasets_get(
    query: Optional[str] = Query(None, description="Search query string"),
    tags: Optional[List[str]] = Query(None, description="Filter by tags"),
    file_types: Optional[List[str]] = Query(None, description="Filter by file types"),
    created_by: Optional[List[int]] = Query(None, description="Filter by creator user IDs"),
    fuzzy: bool = Query(False, description="Enable fuzzy search"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    sort_by: str = Query("relevance", description="Sort field"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$", description="Sort order"),
    include_facets: bool = Query(True, description="Include facet counts"),
    service: SearchServiceDep = None,
    current_user: UserDep = None
) -> SearchResponse:
    """
    Search datasets using GET request with query parameters.
    This is a simplified version of the POST endpoint for basic searches.
    """
    # Convert to SearchRequest
    request = SearchRequest(
        query=query,
        tags=tags,
        file_types=file_types,
        created_by=created_by,
        fuzzy_search=fuzzy,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order,
        include_facets=include_facets
    )
    
    try:
        response = await service.search_datasets(request, current_user)
        return response
    except Exception as e:
        logger.error(f"Search error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during search"
        )


@router.post(
    "/suggest",
    response_model=SearchSuggestResponse,
    summary="Get search suggestions",
    description="Get autocomplete suggestions for search queries"
)
async def get_search_suggestions(
    request: SearchSuggestRequest = Body(..., description="Suggestion request"),
    service: SearchServiceDep = None,
    current_user: UserDep = None
) -> SearchSuggestResponse:
    """
    Get search suggestions/autocomplete results based on partial query.
    
    Returns suggestions from:
    - Dataset names
    - Tags
    - User names (if enabled)
    """
    try:
        response = await service.get_suggestions(request)
        return response
    except Exception as e:
        logger.error(f"Suggestion error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred getting suggestions"
        )


@router.get(
    "/suggest",
    response_model=SearchSuggestResponse,
    summary="Get search suggestions (GET)",
    description="Get autocomplete suggestions using query parameters"
)
async def get_search_suggestions_get(
    query: str = Query(..., min_length=1, description="Partial search query"),
    limit: int = Query(10, ge=1, le=50, description="Maximum suggestions"),
    types: Optional[List[str]] = Query(None, description="Types to include"),
    service: SearchServiceDep = None,
    current_user: UserDep = None
) -> SearchSuggestResponse:
    """
    Get search suggestions using GET request.
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


@router.post(
    "/init",
    summary="Initialize search capabilities",
    description="Initialize database extensions and indexes for search (admin only)"
)
async def initialize_search(
    service: SearchServiceDep = None,
    current_user: UserDep = None
) -> dict:
    """
    Initialize search capabilities in the database.
    This creates necessary extensions and indexes.
    Should only be run by administrators.
    """
    # Check if user is admin
    if not current_user.is_admin():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin permission required"
        )
    
    try:
        await service.ensure_search_capabilities()
        return {
            "status": "success",
            "message": "Search capabilities initialized successfully"
        }
    except Exception as e:
        logger.error(f"Search initialization error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initialize search: {str(e)}"
        )