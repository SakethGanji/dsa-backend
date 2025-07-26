"""Handler for searching datasets."""

from typing import Optional, List, Dict, Any
from datetime import datetime

from ...base_handler import BaseHandler
from ....infrastructure.postgres.uow import PostgresUnitOfWork
from ..models.search_request import SearchRequest
from ..models.search_response import SearchResponse
from src.core.domain_exceptions import ValidationException


class SearchDatasetsHandler(BaseHandler[SearchResponse]):
    """Handler for executing dataset searches."""
    
    def __init__(self, unit_of_work: PostgresUnitOfWork):
        """Initialize the handler with a unit of work."""
        super().__init__(unit_of_work)

    async def handle(self, request: SearchRequest) -> SearchResponse:
        """
        Execute a dataset search.
        
        Args:
            request: The search request containing query and filters
            
        Returns:
            SearchResponse with results and metadata
        """
        async with self._uow as uow:
            # Get current user ID from context
            user_id = request.context.get('user_id')
            if not user_id:
                raise ValidationException("User ID not found in request context")
            
            # Execute search through repository
            result = await uow.search_repository.search(
                user_id=user_id,
                query=request.query,
                fuzzy=request.fuzzy,
                tags=request.tags,
                created_by=request.created_by,
                created_after=request.created_after,
                created_before=request.created_before,
                updated_after=request.updated_after,
                updated_before=request.updated_before,
                limit=request.limit,
                offset=request.offset,
                sort_by=request.sort_by,
                sort_order=request.sort_order,
                include_facets=request.include_facets,
                facet_fields=request.facet_fields
            )
            
            # Convert the result dictionary to SearchResponse
            return SearchResponse(**result)