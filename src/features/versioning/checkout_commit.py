"""Handler for checking out a specific commit."""

from typing import Optional
from ...core.abstractions.uow import IUnitOfWork
from ...models.pydantic_models import CheckoutResponse
from ..base_handler import BaseHandler, with_error_handling, PaginationMixin


class CheckoutCommitHandler(BaseHandler[CheckoutResponse], PaginationMixin):
    """Handler for retrieving data at a specific commit."""
    
    def __init__(self, uow: IUnitOfWork):
        super().__init__(uow)
        
    @with_error_handling
    async def handle(self, dataset_id: int, commit_id: str, 
                    table_key: Optional[str] = None,
                    offset: int = 0, limit: int = 100) -> CheckoutResponse:
        """Get data as it existed at a specific commit."""
        # Validate pagination parameters
        offset, limit = self.validate_pagination(offset, limit)
        
        async with self._uow:
            # Verify commit belongs to dataset
            commit = await self._uow.commits.get_commit_by_id(commit_id)
            if not commit or commit['dataset_id'] != dataset_id:
                raise ValueError("Commit not found for this dataset")
            
            # Get data using existing method
            data_rows = await self._uow.commits.get_commit_data(
                commit_id=commit_id,
                table_key=table_key,
                offset=offset,
                limit=limit
            )
            
            # Extract just the data portion for response
            data = []
            for row in data_rows:
                row_data = row['data']
                # Add the logical_row_id to help identify rows
                row_data['_logical_row_id'] = row['logical_row_id']
                data.append(row_data)
            
            # Get total count
            total_rows = await self._uow.commits.count_commit_rows(commit_id, table_key)
            
            return CheckoutResponse(
                commit_id=commit_id,
                data=data,
                total_rows=total_rows,
                offset=offset,
                limit=limit,
                has_more=offset + len(data) < total_rows
            )