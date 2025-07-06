"""Search repository interface for dataset search functionality."""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import datetime


class ISearchRepository(ABC):
    """Interface for search operations on datasets."""

    @abstractmethod
    async def search(
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
    ) -> Dict[str, Any]:
        """
        Execute a search query with filters and return results with metadata.
        
        Args:
            user_id: ID of the user performing the search
            query: Main search query text
            fuzzy: Whether to use fuzzy/typo-tolerant search
            tags: Filter by datasets containing ALL of these tags
            created_by: Filter by creator user IDs
            created_after: Filter by creation date after this time
            created_before: Filter by creation date before this time
            updated_after: Filter by update date after this time
            updated_before: Filter by update date before this time
            limit: Number of results to return (1-100)
            offset: Number of results to skip for pagination
            sort_by: Sort field ('relevance', 'name', 'created_at', 'updated_at')
            sort_order: Sort order ('asc', 'desc')
            include_facets: Whether to include facet counts
            facet_fields: Which facets to calculate
            
        Returns:
            Dictionary containing search results and metadata
        """
        pass

    @abstractmethod
    async def suggest(
        self,
        user_id: int,
        query: str,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Get autocomplete suggestions for a partial query.
        
        Args:
            user_id: ID of the user requesting suggestions
            query: Partial text to get suggestions for
            limit: Maximum number of suggestions (1-50)
            
        Returns:
            Dictionary containing suggestions and metadata
        """
        pass

    @abstractmethod
    async def refresh_search_index(self) -> bool:
        """
        Refresh the materialized view for search.
        
        Returns:
            True if refresh was successful
        """
        pass