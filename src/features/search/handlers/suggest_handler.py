"""Handler for autocomplete suggestions."""

from ...base_handler import BaseHandler
from ....core.abstractions.uow import IUnitOfWork
from ..models.search_request import SuggestRequest
from ..models.search_response import SuggestResponse


class SuggestHandler(BaseHandler[SuggestResponse]):
    """Handler for getting autocomplete suggestions."""
    
    def __init__(self, unit_of_work: IUnitOfWork):
        """Initialize the handler with a unit of work."""
        super().__init__(unit_of_work)

    async def handle(self, request: SuggestRequest) -> SuggestResponse:
        """
        Get autocomplete suggestions for a partial query.
        
        Args:
            request: The suggest request containing the partial query
            
        Returns:
            SuggestResponse with suggestions
        """
        async with self._uow as uow:
            # Get current user ID from context
            user_id = request.context.get('user_id')
            if not user_id:
                raise ValueError("User ID not found in request context")
            
            # Get suggestions through repository
            result = await uow.search_repository.suggest(
                user_id=user_id,
                query=request.query,
                limit=request.limit
            )
            
            # Convert the result dictionary to SuggestResponse
            return SuggestResponse(**result)