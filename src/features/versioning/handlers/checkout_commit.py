"""Handler for checking out a specific commit."""

from typing import Optional
from ....infrastructure.postgres.uow import PostgresUnitOfWork
from ....api.models.responses import GetDataResponse
from ...base_handler import BaseHandler, with_error_handling
from ....core.common.pagination import PaginationMixin
from ....core.domain_exceptions import EntityNotFoundException


class CheckoutCommitHandler(BaseHandler[GetDataResponse], PaginationMixin):
    """Handler for retrieving data at a specific commit."""
    
    def __init__(self, uow: PostgresUnitOfWork):
        super().__init__(uow)
        
    @with_error_handling
    async def handle(self, dataset_id: int, commit_id: str, 
                    table_key: Optional[str] = None,
                    offset: int = 0, limit: int = 100) -> GetDataResponse:
        """Get data as it existed at a specific commit."""
        # Validate pagination parameters
        offset, limit = self.validate_pagination(offset, limit)
        
        async with self._uow:
            # Verify commit belongs to dataset
            commit = await self._uow.commits.get_commit_by_id(commit_id)
            if not commit or commit['dataset_id'] != dataset_id:
                raise EntityNotFoundException("Commit", commit_id)
            
            # Get data using ITableReader
            if table_key:
                # Get data for specific table
                data_rows = await self._uow.table_reader.get_table_data(
                    commit_id=commit_id,
                    table_key=table_key,
                    offset=offset,
                    limit=limit
                )
                total_rows = await self._uow.table_reader.count_table_rows(commit_id, table_key)
            else:
                # Get data from all tables - first list all tables
                table_keys = await self._uow.table_reader.list_table_keys(commit_id)
                
                # For simplicity, get data from the first table or 'primary' if it exists
                default_table_key = 'primary' if 'primary' in table_keys else (table_keys[0] if table_keys else None)
                
                if default_table_key:
                    data_rows = await self._uow.table_reader.get_table_data(
                        commit_id=commit_id,
                        table_key=default_table_key,
                        offset=offset,
                        limit=limit
                    )
                    total_rows = await self._uow.table_reader.count_table_rows(commit_id, default_table_key)
                else:
                    data_rows = []
                    total_rows = 0
            
            # Extract just the data portion for response
            data = []
            for row in data_rows:
                # Remove internal fields starting with underscore
                row_data = {k: v for k, v in row.items() if not k.startswith('_')}
                # Add the logical_row_id to help identify rows
                row_data['_logical_row_id'] = row.get('_logical_row_id', '')
                data.append(row_data)
            
            return GetDataResponse(
                dataset_id=dataset_id,
                ref_name="",  # Not available in this context
                commit_id=commit_id,
                rows=data,
                total_rows=total_rows,
                offset=offset,
                limit=limit
            )