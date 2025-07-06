"""Handler for refreshing the search index."""

from ...base_handler import BaseHandler
from ....core.abstractions.uow import IUnitOfWork


class RefreshSearchIndexHandler(BaseHandler[dict]):
    """Handler for refreshing the search materialized view."""
    
    def __init__(self, unit_of_work: IUnitOfWork):
        """Initialize the handler with a unit of work."""
        super().__init__(unit_of_work)

    async def handle(self, request: dict) -> dict:
        """
        Refresh the search index (materialized view).
        
        Args:
            request: Empty dictionary (no parameters needed)
            
        Returns:
            Dictionary with success status and message
        """
        async with self._uow as uow:
            success = await uow.search_repository.refresh_search_index()
            
            # Commit is not needed for REFRESH MATERIALIZED VIEW
            # as it's a DDL operation that auto-commits
            
            return {
                "success": success,
                "message": "Search index refreshed successfully" if success else "Failed to refresh search index"
            }