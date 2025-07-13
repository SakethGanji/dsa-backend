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
                    permission_type='read'
                )
                if not has_permission:
                    raise PermissionError(f"No read permission for dataset {source.dataset_id}")
    
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
                    elif 'data' in first_row and isinstance(first_row['data'], str):
                        # If data is a JSON string, parse it to get columns
                        try:
                            parsed_data = json.loads(first_row['data'])
                            columns = [
                                {"name": key, "type": self._get_pg_type_name(type(value))}
                                for key, value in parsed_data.items()
                            ]
                        except json.JSONDecodeError:
                            columns = [
                                {"name": key, "type": self._get_pg_type_name(type(value))}
                                for key, value in first_row.items()
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
                        # Use the nested data directly
                        data.append(row_dict['data'])
                    elif 'data' in row_dict and isinstance(row_dict['data'], str):
                        # If data is a JSON string, parse it
                        try:
                            parsed_data = json.loads(row_dict['data'])
                            data.append(parsed_data)
                        except json.JSONDecodeError:
                            data.append(row_dict)
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
        
        # Create view using commit data
        # This is simplified - in production would use table reader
        # Note: Parameters can't be used in CREATE VIEW, so we use string formatting
        # Extract and parse the nested data field - it's stored as a JSON string
        view_sql = f"""
            CREATE TEMPORARY VIEW {view_name} AS
            SELECT 
                CASE 
                    WHEN jsonb_typeof(r.data->'data') = 'string' THEN (r.data->>'data')::jsonb
                    ELSE r.data->'data'
                END AS data
            FROM dsa_core.commit_rows cr
            JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
            WHERE cr.commit_id = '{commit_id}'
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