"""Handler for getting exploration history."""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from src.features.base_handler import BaseHandler
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from ..models import GetExplorationHistoryCommand


@dataclass 
class ExplorationHistoryItem:
    """Single item in exploration history."""
    job_id: str
    dataset_id: int
    dataset_name: str
    user_id: int
    username: str
    status: str
    created_at: str
    updated_at: Optional[str]
    run_parameters: Dict[str, Any]
    has_result: bool


@dataclass
class ExplorationHistoryResponse:
    """Response for exploration history."""
    items: List[ExplorationHistoryItem]
    total: int
    offset: int
    limit: int


class GetExplorationHistoryHandler(BaseHandler[ExplorationHistoryResponse]):
    """Handler for getting exploration history."""
    
    def __init__(self, uow: PostgresUnitOfWork):
        super().__init__(uow)
    
    async def handle(self, command: GetExplorationHistoryCommand) -> ExplorationHistoryResponse:
        """Get exploration history."""
        async with self._uow:
            # Get exploration history
            history_items = await self._uow.explorations.get_exploration_history(
                dataset_id=command.dataset_id,
                user_id=command.user_id,
                limit=command.limit,
                offset=command.offset
            )
            
            # Convert to response items
            items = [
                ExplorationHistoryItem(
                    job_id=item["job_id"],
                    dataset_id=item["dataset_id"],
                    dataset_name=item["dataset_name"],
                    user_id=item["user_id"],
                    username=item["username"],
                    status=item["status"],
                    created_at=item["created_at"],
                    updated_at=item["updated_at"],
                    run_parameters=item["run_parameters"],
                    has_result=item["has_result"]
                )
                for item in history_items
            ]
            
            # Get total count
            total = await self._uow.explorations.count_explorations(
                dataset_id=command.dataset_id,
                user_id=command.user_id
            )
            
            return ExplorationHistoryResponse(
                items=items,
                total=total,
                offset=command.offset,
                limit=command.limit
            )