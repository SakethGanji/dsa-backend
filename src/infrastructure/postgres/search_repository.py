"""PostgreSQL implementation of the search repository."""

from typing import List, Optional, Dict, Any
from datetime import datetime
import json
import asyncpg

from src.core.abstractions.search_repository import ISearchRepository


class PostgresSearchRepository(ISearchRepository):
    """PostgreSQL implementation of search repository using native full-text search."""
    
    def __init__(self, connection: asyncpg.Connection):
        """Initialize the repository with a database connection."""
        self._connection = connection

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
        """Execute a search query using the PostgreSQL search function."""
        if facet_fields is None:
            facet_fields = ['tags', 'created_by']
            
        # Call the PostgreSQL function
        result = await self._connection.fetchval(
            """
            SELECT dsa_search.search(
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                $11, $12, $13, $14, $15
            )
            """,
            user_id,
            query,
            fuzzy,
            tags,
            created_by,
            created_after,
            created_before,
            updated_after,
            updated_before,
            limit,
            offset,
            sort_by,
            sort_order,
            include_facets,
            facet_fields
        )
        
        # The PostgreSQL function returns JSONB, parse if it's a string
        if isinstance(result, str):
            return json.loads(result)
        return result

    async def suggest(
        self,
        user_id: int,
        query: str,
        limit: int = 10
    ) -> Dict[str, Any]:
        """Get autocomplete suggestions using the PostgreSQL suggest function."""
        result = await self._connection.fetchval(
            "SELECT dsa_search.suggest($1, $2, $3)",
            user_id, 
            query, 
            limit
        )
        
        # The PostgreSQL function returns JSONB, parse if it's a string
        if isinstance(result, str):
            return json.loads(result)
        return result

    async def refresh_search_index(self) -> bool:
        """Refresh the search materialized view."""
        try:
            await self._connection.execute(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_search.datasets_summary"
            )
            return True
        except Exception:
            # Log the error in production
            return False