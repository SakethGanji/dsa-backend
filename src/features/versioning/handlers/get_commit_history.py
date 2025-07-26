"""Handler for retrieving commit history."""

from typing import List
from ....infrastructure.postgres.uow import PostgresUnitOfWork
from ....api.models.responses import GetCommitHistoryResponse
from ....api.models.common import CommitInfo
from ...base_handler import BaseHandler, with_error_handling
from ....core.common.pagination import PaginationMixin


class GetCommitHistoryHandler(BaseHandler[GetCommitHistoryResponse], PaginationMixin):
    """Handler for getting commit history of a dataset."""
    
    def __init__(self, uow: PostgresUnitOfWork):
        super().__init__(uow)
    
    @with_error_handling
    async def handle(self, dataset_id: int, ref_name: str = "main", offset: int = 0, limit: int = 50) -> GetCommitHistoryResponse:
        """Get paginated commit history for a specific ref."""
        # Validate pagination parameters
        offset, limit = self.validate_pagination(offset, limit)
        
        async with self._uow:
            # Get commits from repository
            commits = await self._uow.commits.get_commit_history(
                dataset_id=dataset_id,
                ref_name=ref_name,
                offset=offset,
                limit=limit
            )
            
            # Enrich with author names
            enriched_commits = []
            for commit in commits:
                user = await self._uow.users.get_by_id(commit['author_id'])
                enriched_commits.append(CommitInfo(
                    commit_id=commit['commit_id'].strip() if commit['commit_id'] else '',  # Trim whitespace
                    parent_commit_id=commit['parent_commit_id'].strip() if commit['parent_commit_id'] else None,
                    message=commit['message'],
                    author_id=commit['author_id'],
                    author_soeid=user['soeid'] if user else 'Unknown',
                    created_at=commit['created_at'],
                    table_count=commit.get('row_count', 0),  # Using row_count as table_count
                    is_head=False  # Will be set later if needed
                ))
            
            # Get total count
            total = await self._uow.commits.count_commits_for_dataset(dataset_id, ref_name)
            
            return GetCommitHistoryResponse(
                dataset_id=dataset_id,
                ref_name=ref_name,
                commits=enriched_commits,
                total=total,
                offset=offset,
                limit=limit
            )