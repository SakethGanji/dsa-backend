"""Handler for retrieving data from a specific table within a dataset."""

from typing import Dict, Any, List, Optional
from fastapi import HTTPException

from src.core.abstractions import IUnitOfWork, ITableReader
from src.features.base_handler import BaseHandler, with_error_handling, PaginationMixin


class GetTableDataHandler(BaseHandler[Dict[str, Any]], PaginationMixin):
    """Handler for retrieving paginated data from a specific table within a dataset."""
    
    def __init__(self, uow: IUnitOfWork, table_reader: ITableReader):
        super().__init__(uow)
        self._table_reader = table_reader
    
    @with_error_handling
    async def handle(
        self,
        dataset_id: int,
        ref_name: str,
        table_key: str,
        user_id: int,
        offset: int = 0,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get paginated data for a specific table within a dataset.
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref name (e.g., 'main')
            table_key: The table key (e.g., 'primary' for Parquet, sheet name for Excel)
            user_id: The requesting user's ID
            offset: Pagination offset
            limit: Maximum rows to return
            
        Returns:
            Dict containing table data and pagination info
        """
        # Validate pagination parameters
        offset, limit = self.validate_pagination(offset, limit)
        
        async with self._uow:
            # Permission check removed - handled by authorization middleware
            
            # Get the current commit for the ref
            ref = await self._uow.commits.get_ref(dataset_id, ref_name)
            if not ref:
                raise HTTPException(
                    status_code=404,
                    detail=f"Ref '{ref_name}' not found"
                )
            
            if not ref['commit_id']:
                return {
                    "table_key": table_key,
                    "data": [],
                    "offset": offset,
                    "limit": limit,
                    "total_count": 0
                }
            
            # Get total count for this table
            total_count = await self._table_reader.count_table_rows(
                ref['commit_id'], table_key
            )
            
            # Get paginated data
            data = await self._table_reader.get_table_data(
                commit_id=ref['commit_id'],
                table_key=table_key,
                offset=offset,
                limit=limit
            )
            
            return {
                "table_key": table_key,
                "data": data,
                "offset": offset,
                "limit": limit,
                "total_count": total_count
            }


class ListTablesHandler(BaseHandler[Dict[str, Any]]):
    """Handler for listing all available tables in a dataset."""
    
    def __init__(self, uow: IUnitOfWork, table_reader: ITableReader):
        super().__init__(uow)
        self._table_reader = table_reader
    
    @with_error_handling
    async def handle(
        self,
        dataset_id: int,
        ref_name: str,
        user_id: int
    ) -> Dict[str, Any]:
        """
        List all available tables in the dataset at the given ref.
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref name
            user_id: The requesting user's ID
            
        Returns:
            Dict containing list of table keys
        """
        async with self._uow:
            # Permission check removed - handled by authorization middleware
            
            # Get the current commit for the ref
            ref = await self._uow.commits.get_ref(dataset_id, ref_name)
            if not ref:
                raise HTTPException(
                    status_code=404,
                    detail=f"Ref '{ref_name}' not found"
                )
            
            if not ref['commit_id']:
                return {"tables": []}
            
            # Get table keys using table reader
            tables = await self._table_reader.list_table_keys(ref['commit_id'])
            
            return {
                "tables": tables
            }


class GetTableSchemaHandler(BaseHandler[Dict[str, Any]]):
    """Handler for retrieving schema information for a specific table."""
    
    def __init__(self, uow: IUnitOfWork, table_reader: ITableReader):
        super().__init__(uow)
        self._table_reader = table_reader
    
    @with_error_handling
    async def handle(
        self,
        dataset_id: int,
        ref_name: str,
        table_key: str,
        user_id: int
    ) -> Dict[str, Any]:
        """
        Get schema for a specific table within a dataset.
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref name
            table_key: The table key
            user_id: The requesting user's ID
            
        Returns:
            Dict containing schema information
        """
        async with self._uow:
            # Permission check removed - handled by authorization middleware
            
            # Get the current commit for the ref
            ref = await self._uow.commits.get_ref(dataset_id, ref_name)
            if not ref:
                raise HTTPException(
                    status_code=404,
                    detail=f"Ref '{ref_name}' not found"
                )
            
            if not ref['commit_id']:
                return {"table_key": table_key, "schema": None}
            
            # Get schema for the specific table
            schema = await self._table_reader.get_table_schema(
                ref['commit_id'], table_key
            )
            
            if not schema:
                raise HTTPException(
                    status_code=404,
                    detail=f"Table '{table_key}' not found in this dataset"
                )
            
            return {
                "table_key": table_key,
                "schema": schema
            }