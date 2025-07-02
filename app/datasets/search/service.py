"""Service layer for dataset search functionality"""
import time
from typing import List, Optional, Dict, Any
from datetime import datetime

from app.datasets.search.models import (
    SearchRequest, SearchResponse, SearchResult, SearchFacets,
    SearchSuggestRequest, SearchSuggestResponse, SearchSuggestion
)
from app.datasets.search.repository import SearchRepository
from app.users.auth import CurrentUser


class SearchService:
    """Service for dataset search operations"""
    
    def __init__(self, search_repository: SearchRepository, user_service):
        self.search_repo = search_repository
        self.user_service = user_service
    
    async def search_datasets(
        self,
        request: SearchRequest,
        current_user: CurrentUser
    ) -> SearchResponse:
        """
        Search datasets with permissions check and faceting.
        """
        start_time = time.time()
        
        # Get user ID from soeid
        user = await self.user_service.get_user_by_soeid(current_user.soeid)
        if not user:
            user_id = None
        else:
            user_id = user.id
        if not user_id:
            # Return empty results if user not found
            return SearchResponse(
                results=[],
                total=0,
                limit=request.limit,
                offset=request.offset,
                has_more=False,
                query=request.query,
                execution_time_ms=0,
                facets=None
            )
        
        # Perform search
        results, total = await self.search_repo.search_datasets(request, user_id)
        
        # Get facets if requested
        facets = None
        if request.include_facets:
            facet_data = await self.search_repo.get_search_facets(request, user_id)
            if facet_data:
                facets = SearchFacets(**facet_data)
        
        # Calculate execution time
        execution_time_ms = (time.time() - start_time) * 1000
        
        # Check if there are more results
        has_more = (request.offset + request.limit) < total
        
        return SearchResponse(
            results=results,
            total=total,
            limit=request.limit,
            offset=request.offset,
            has_more=has_more,
            query=request.query,
            execution_time_ms=execution_time_ms,
            facets=facets
        )
    
    async def get_suggestions(
        self,
        request: SearchSuggestRequest
    ) -> SearchSuggestResponse:
        """
        Get search suggestions/autocomplete results.
        """
        start_time = time.time()
        
        suggestions = await self.search_repo.get_search_suggestions(
            query=request.query,
            limit=request.limit,
            types=request.types
        )
        
        execution_time_ms = (time.time() - start_time) * 1000
        
        return SearchSuggestResponse(
            suggestions=suggestions,
            query=request.query,
            execution_time_ms=execution_time_ms
        )
    
    
    def validate_search_request(self, request: SearchRequest) -> None:
        """
        Validate search request parameters.
        """
        # Validate date ranges
        if request.created_at:
            if request.created_at.start and request.created_at.end:
                if request.created_at.start > request.created_at.end:
                    raise ValueError("Created date start must be before end")
        
        if request.updated_at:
            if request.updated_at.start and request.updated_at.end:
                if request.updated_at.start > request.updated_at.end:
                    raise ValueError("Updated date start must be before end")
        
        # Validate numeric ranges
        if request.file_size:
            if request.file_size.min is not None and request.file_size.max is not None:
                if request.file_size.min > request.file_size.max:
                    raise ValueError("File size min must be less than max")
        
        if request.version_count:
            if request.version_count.min is not None and request.version_count.max is not None:
                if request.version_count.min > request.version_count.max:
                    raise ValueError("Version count min must be less than max")
        
        # Validate facet fields
        valid_facet_fields = ['tags', 'file_types', 'created_by', 'years']
        if request.facet_fields:
            invalid_fields = [f for f in request.facet_fields if f not in valid_facet_fields]
            if invalid_fields:
                raise ValueError(f"Invalid facet fields: {invalid_fields}")