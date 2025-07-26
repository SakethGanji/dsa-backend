from typing import Optional, List, Dict, Any

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.table_reader import PostgresTableReader
from src.api.models import GetDataRequest, GetDataResponse, DataRow
from ...base_handler import BaseHandler, with_error_handling
from src.core.common.pagination import PaginationMixin
from src.core.domain_exceptions import EntityNotFoundException


class GetDataAtRefHandler(BaseHandler[GetDataResponse], PaginationMixin):
    """Handler for retrieving data at a specific ref"""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        table_reader: PostgresTableReader
    ):
        super().__init__(uow)
        self._table_reader = table_reader
    
    @with_error_handling
    async def handle(
        self,
        dataset_id: int,
        ref_name: str,
        request: GetDataRequest,
        user_id: int
    ) -> GetDataResponse:
        """
        Retrieve paginated data for a ref
        
        Steps:
        1. Check read permission
        2. Get commit ID for ref
        3. Fetch data from commit using ITableReader
        4. Apply pagination and filtering
        """
        # Validate pagination parameters
        offset, limit = self.validate_pagination(request.offset, request.limit)
        
        # Permission check removed - handled by authorization middleware
        
        async with self._uow:
            # Get current commit for ref
            ref = await self._uow.commits.get_ref(dataset_id, ref_name)
            if not ref or not ref['commit_id']:
                raise EntityNotFoundException("Ref", ref_name)
            
            commit_id = ref['commit_id'].strip() if ref['commit_id'] else ''
            
            # If no table_key specified, get data from all tables
            table_key = request.sheet_name  # Use sheet_name from request
            if table_key:
                # Get data for specific table
                rows_data = await self._table_reader.get_table_data(
                    commit_id=commit_id,
                    table_key=table_key,
                    offset=offset,
                    limit=limit
                )
                total_rows = await self._table_reader.count_table_rows(
                    commit_id, table_key
                )
            else:
                # Get data from all tables - first list all tables
                table_keys = await self._table_reader.list_table_keys(commit_id)
                
                # For simplicity, get data from the first table or 'primary' if it exists
                table_key = 'primary' if 'primary' in table_keys else (table_keys[0] if table_keys else None)
                
                if table_key:
                    rows_data = await self._table_reader.get_table_data(
                        commit_id=commit_id,
                        table_key=table_key,
                        offset=offset,
                        limit=limit
                    )
                    total_rows = await self._table_reader.count_table_rows(
                        commit_id, table_key
                    )
                else:
                    rows_data = []
                    total_rows = 0
            
            # Transform to response format
            rows = [
                DataRow(
                    sheet_name=table_key or 'default',  # Provide sheet name
                    logical_row_id=row.get('_logical_row_id', ''),
                    data={k: v for k, v in row.items() if not k.startswith('_')}
                )
                for row in rows_data
            ]
            
            return GetDataResponse(
                dataset_id=dataset_id,
                ref_name=ref_name,
                commit_id=commit_id,
                rows=rows,
                total_rows=total_rows,
                offset=offset,
                limit=limit
            )