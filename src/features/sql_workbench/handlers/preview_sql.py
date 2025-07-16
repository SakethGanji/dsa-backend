"""Handler for previewing SQL query results."""

import time
from typing import Dict, Any, List
import asyncpg
import json

from ...base_handler import BaseHandler, with_error_handling, with_transaction
from ....core.abstractions.uow import IUnitOfWork
from ....core.abstractions.services import IWorkbenchService, WorkbenchContext, OperationType
from ....core.abstractions.repositories import IDatasetRepository, ITableReader
# Use standard Python exceptions instead of custom error classes
from ..models.sql_preview import SqlPreviewRequest, SqlPreviewResponse, SqlSource
from src.core.domain_exceptions import ForbiddenException


class PreviewSqlHandler(BaseHandler[SqlPreviewResponse]):
    """Handler for executing SQL preview queries."""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        workbench_service: IWorkbenchService,
        table_reader: ITableReader,
        dataset_repository: IDatasetRepository
    ):
        """Initialize handler with dependencies."""
        super().__init__(uow)
        self._workbench_service = workbench_service
        self._table_reader = table_reader
        self._dataset_repository = dataset_repository
    
    @with_error_handling
    async def handle(
        self,
        request: SqlPreviewRequest,
        user_id: int
    ) -> SqlPreviewResponse:
        """
        Preview SQL query results.
        
        Args:
            request: SQL preview request with sources and query
            user_id: ID of the user executing the query
            
        Returns:
            SqlPreviewResponse with query results
        """
        # Validate permissions for all source datasets
        await self._validate_permissions(request.sources, user_id)
        
        # Create workbench context
        context = WorkbenchContext(
            user_id=user_id,
            source_datasets=[s.dataset_id for s in request.sources],
            source_refs=[s.ref for s in request.sources],
            operation_type=OperationType.SQL_TRANSFORM,
            parameters={
                "sql": request.sql,
                "sources": [s.dict() for s in request.sources],
                "limit": request.limit
            }
        )
        
        # Validate operation
        async with self._uow:
            errors = await self._workbench_service.validate_operation(context, self._uow)
            if errors:
                raise ValueError(f"SQL validation failed: {'; '.join(errors)}")
        
        # Execute preview
        start_time = time.time()
        
        # Create temporary views and execute query
        result = await self._execute_preview_query(request)
        
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        return SqlPreviewResponse(
            data=result['data'],
            row_count=len(result['data']),
            total_row_count=result.get('total_row_count'),
            execution_time_ms=execution_time_ms,
            columns=result['columns'],
            truncated=result.get('truncated', False)
        )
    
    async def _validate_permissions(self, sources: List[SqlSource], user_id: int):
        """Validate user has read permissions for all source datasets."""
        async with self._uow:
            for source in sources:
                dataset = await self._dataset_repository.get_dataset_by_id(source.dataset_id)
                if not dataset:
                    raise KeyError(f"Dataset {source.dataset_id} not found")
                
                # Check read permission
                has_permission = await self._dataset_repository.check_user_permission(
                    dataset_id=source.dataset_id,
                    user_id=user_id,
                    required_permission='read'
                )
                if not has_permission:
                    raise ForbiddenException()
    
    async def _execute_preview_query(self, request: SqlPreviewRequest) -> Dict[str, Any]:
        """Execute the SQL query with temporary views for source tables."""
        # Get database connection from unit of work
        async with self._uow:
            conn = self._uow.connection
            
            # Create temporary views for each source
            view_names = []
            try:
                for source in request.sources:
                    view_name = f"temp_view_{source.alias}_{int(time.time())}"
                    view_names.append((source.alias, view_name))
                    
                    # Create view using table reader's data
                    await self._create_temp_view(
                        conn,
                        view_name,
                        source,
                        limit=request.limit
                    )
                
                # Replace aliases with view names in SQL
                modified_sql = request.sql
                for alias, view_name in view_names:
                    # Simple replacement - in production would use SQL parser
                    modified_sql = modified_sql.replace(f" {alias} ", f" {view_name} ")
                    modified_sql = modified_sql.replace(f" {alias}.", f" {view_name}.data.")
                    modified_sql = modified_sql.replace(f"FROM {alias}", f"FROM {view_name}")
                    modified_sql = modified_sql.replace(f"JOIN {alias}", f"JOIN {view_name}")
                    # Handle SELECT * case for JSONB data
                    if f"SELECT * FROM {view_name}" in modified_sql or f"SELECT *\nFROM {view_name}" in modified_sql:
                        modified_sql = modified_sql.replace("SELECT *", "SELECT data")
                
                # Add LIMIT if not present
                if 'LIMIT' not in modified_sql.upper():
                    modified_sql = f"{modified_sql} LIMIT {request.limit}"
                
                # Execute query
                rows = await conn.fetch(modified_sql)
                
                # Get column info
                columns = []
                if rows:
                    first_row = dict(rows[0])
                    # If we have a data column with JSONB, get columns from the nested data
                    if 'data' in first_row and isinstance(first_row['data'], dict):
                        columns = [
                            {"name": key, "type": self._get_pg_type_name(type(value))}
                            for key, value in first_row['data'].items()
                        ]
                    else:
                        columns = [
                            {"name": key, "type": self._get_pg_type_name(type(value))}
                            for key, value in first_row.items()
                        ]
                
                # Convert rows to list of dicts and expand JSONB data
                data = []
                for row in rows:
                    row_dict = dict(row)
                    # If we have a 'data' column that's a dict, expand it
                    if 'data' in row_dict and isinstance(row_dict['data'], dict):
                        # Use the nested data directly (standardized format)
                        data.append(row_dict['data'])
                    else:
                        # Otherwise, handle JSON serialization for complex types
                        for key, value in row_dict.items():
                            if isinstance(value, (dict, list)):
                                row_dict[key] = json.dumps(value)
                        data.append(row_dict)
                
                return {
                    "data": data,
                    "columns": columns,
                    "truncated": len(data) == request.limit,
                    "total_row_count": None  # Would need COUNT query for total
                }
                
            finally:
                # Clean up temporary views
                for _, view_name in view_names:
                    await conn.execute(f"DROP VIEW IF EXISTS {view_name}")
    
    async def _create_temp_view(
        self,
        conn: asyncpg.Connection,
        view_name: str,
        source: SqlSource,
        limit: int
    ):
        """Create a temporary view for a source table."""
        # Get commit ID for the ref
        commit_query = """
            SELECT c.commit_id 
            FROM dsa_core.refs r
            JOIN dsa_core.commits c ON r.commit_id = c.commit_id
            WHERE r.dataset_id = $1 AND r.name = $2
        """
        commit_row = await conn.fetchrow(commit_query, source.dataset_id, source.ref)
        if not commit_row:
            raise KeyError(f"Ref '{source.ref}' not found for dataset {source.dataset_id}")
        
        commit_id = commit_row['commit_id']
        
        # Create view using commit data filtered by table_key
        # This filters rows to only include those from the specified table
        # Note: Parameters can't be used in CREATE VIEW, so we use string formatting
        # We need to escape single quotes in table_key for SQL safety
        escaped_table_key = source.table_key.replace("'", "''")
        
        view_sql = f"""
            CREATE TEMPORARY VIEW {view_name} AS
            SELECT 
                cr.logical_row_id,
                CASE 
                    WHEN jsonb_typeof(r.data->'data') = 'string' THEN (r.data->>'data')::jsonb
                    ELSE r.data->'data'
                END AS data
            FROM dsa_core.commit_rows cr
            JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
            WHERE cr.commit_id = '{commit_id}'
            AND (cr.logical_row_id LIKE '{escaped_table_key}:%' 
                 OR cr.logical_row_id LIKE '{escaped_table_key}\\_%')
            LIMIT {limit}
        """
        await conn.execute(view_sql)
    
    def _get_pg_type_name(self, python_type) -> str:
        """Map Python type to PostgreSQL type name."""
        type_map = {
            int: "INTEGER",
            float: "DOUBLE PRECISION",
            str: "TEXT",
            bool: "BOOLEAN",
            dict: "JSONB",
            list: "JSONB",
            type(None): "NULL"
        }
        return type_map.get(python_type, "TEXT")