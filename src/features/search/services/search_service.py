"""Consolidated service for all search operations."""

from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass
import logging

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.domain_exceptions import ValidationException
from ...base_handler import with_error_handling
from ..models import (
    SearchRequest,
    SuggestRequest,
    SearchResponse,
    SuggestResponse
)

logger = logging.getLogger(__name__)


class SearchService:
    """Consolidated service for all search operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork
    ):
        self._uow = uow
    
    # ========== Query Methods ==========
    
    @with_error_handling
    async def search_datasets(
        self,
        user_id: int,
        query: Optional[str] = None,
        fuzzy: bool = True,
        tags: Optional[List[str]] = None,
        created_by: Optional[List[int]] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        updated_after: Optional[datetime] = None,
        updated_before: Optional[datetime] = None,
        limit: int = 20,
        offset: int = 0,
        sort_by: str = 'relevance',
        sort_order: str = 'desc',
        include_facets: bool = True,
        facet_fields: Optional[List[str]] = None
    ) -> SearchResponse:
        """
        Execute a dataset search with advanced filtering and faceting.
        
        Supports:
        - Full-text search with fuzzy matching
        - Filtering by tags, creator, and date ranges
        - Sorting by relevance, name, or timestamps
        - Faceted search for discovering filter options
        - Special query syntax: 'tag:finance', 'user:jsmith'
        """
        # Validate sort parameters
        valid_sort_by = ['relevance', 'name', 'created_at', 'updated_at']
        if sort_by not in valid_sort_by:
            raise ValidationException(f"Invalid sort_by. Must be one of: {', '.join(valid_sort_by)}")
        
        if sort_order not in ['asc', 'desc']:
            raise ValidationException("Invalid sort_order. Must be 'asc' or 'desc'")
        
        # Default facet fields
        if facet_fields is None:
            facet_fields = ['tags', 'created_by']
        
        # Validate facet fields
        valid_facet_fields = ['tags', 'created_by']
        for field in facet_fields:
            if field not in valid_facet_fields:
                raise ValidationException(f"Invalid facet field: {field}. Must be one of: {', '.join(valid_facet_fields)}")
        
        async with self._uow as uow:
            # Execute search through repository
            result = await uow.search_repository.search(
                user_id=user_id,
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
            
            # Convert the result dictionary to SearchResponse
            return SearchResponse(**result)
    
    @with_error_handling
    async def suggest(
        self,
        user_id: int,
        query: str,
        limit: int = 10
    ) -> SuggestResponse:
        """
        Get autocomplete suggestions for dataset names and tags.
        
        Returns suggestions based on:
        - Dataset names that match the partial query
        - Tag names that match the partial query
        
        Suggestions are sorted by relevance and filtered by user permissions.
        """
        # Validate input
        if not query or len(query) < 1:
            raise ValidationException("Query must be at least 1 character")
        
        if limit < 1 or limit > 50:
            raise ValidationException("Limit must be between 1 and 50")
        
        async with self._uow as uow:
            # Get suggestions through repository
            result = await uow.search_repository.suggest(
                user_id=user_id,
                query=query,
                limit=limit
            )
            
            # Convert the result dictionary to SuggestResponse
            return SuggestResponse(**result)
    
    # ========== Index Management Methods ==========
    
    @with_error_handling
    async def refresh_search_index(self) -> Dict[str, Any]:
        """
        Manually refresh the search materialized view.
        
        This is typically called:
        - After bulk imports
        - During maintenance windows
        - When immediate index updates are needed
        
        Note: REFRESH MATERIALIZED VIEW is a DDL operation that auto-commits.
        """
        logger.info("Starting search index refresh")
        async with self._uow as uow:
            success = await uow.search_repository.refresh_search_index()
            
            # Commit is not needed for REFRESH MATERIALIZED VIEW
            # as it's a DDL operation that auto-commits
            
            if success:
                logger.info("Search index refresh completed successfully")
            else:
                logger.error("Search index refresh failed - check repository logs for details")
            
            return {
                "success": success,
                "message": "Search index refreshed successfully" if success else "Failed to refresh search index"
            }
    
    # ========== Event Handler Helper Methods ==========
    
    async def handle_dataset_created(self, dataset_id: int) -> None:
        """
        Handle dataset creation by updating search index.
        
        Note: In production, consider batching these updates for better performance.
        """
        # For now, we refresh the entire index
        # In future, consider incremental updates
        await self.refresh_search_index()
    
    async def handle_dataset_updated(self, dataset_id: int, changes: Dict[str, Any]) -> None:
        """
        Handle dataset update by refreshing search index if searchable fields changed.
        
        Only refreshes if fields that affect search results have changed.
        """
        # Only refresh if searchable fields changed
        searchable_fields = {'name', 'description', 'tags'}
        if any(field in changes for field in searchable_fields):
            await self.refresh_search_index()
    
    async def handle_dataset_deleted(self, dataset_id: int) -> None:
        """
        Handle dataset deletion by updating search index.
        
        The materialized view will automatically exclude deleted datasets
        on next refresh since they won't exist in the source tables.
        """
        await self.refresh_search_index()