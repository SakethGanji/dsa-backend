"""Handler for previewing SQL query results."""
import time
from typing import List

from ...base_handler import BaseHandler, with_error_handling
from ....infrastructure.postgres.uow import PostgresUnitOfWork
from src.services.workbench_service import WorkbenchService
from ....core.permissions import PermissionService, PermissionCheck
from ..models.sql_preview import SqlPreviewRequest, SqlPreviewResponse, SqlSource


class PreviewSqlHandler(BaseHandler[SqlPreviewResponse]):
    """Handler for executing SQL preview queries."""
    
    def __init__(self, uow: PostgresUnitOfWork, workbench_service: WorkbenchService, permissions: PermissionService):
        super().__init__(uow)
        self._workbench_service = workbench_service
        self._permissions = permissions
    
    @with_error_handling
    async def handle(self, request: SqlPreviewRequest, user_id: int) -> SqlPreviewResponse:
        """Preview SQL query results."""
        # Validate permissions
        await self._validate_permissions(request.sources, user_id)
        
        # Build source information for SQL execution
        sources = []
        for source in request.sources:
            ref = await self._uow.commits.get_ref(source.dataset_id, source.ref)
            if not ref:
                raise ValueError(f"Ref '{source.ref}' not found")
            
            sources.append({
                'alias': source.alias,
                'dataset_id': source.dataset_id,
                'commit_id': ref['commit_id'],
                'table_key': source.table_key
            })
        
        # Add LIMIT to the SQL for preview
        limited_sql = f"SELECT * FROM ({request.sql}) AS preview_result LIMIT {request.limit}"
        
        # Execute preview using the new method
        start = time.time()
        
        # Set db_pool on workbench service if needed
        if not self._workbench_service._db_pool:
            self._workbench_service._db_pool = self._uow._pool
        
        result = await self._workbench_service._execute_sql_with_sources(
            sql=limited_sql,
            sources=sources,
            db_pool=self._uow._pool
        )
        
        execution_time_ms = int((time.time() - start) * 1000)
        
        # Convert result format
        data = []
        columns = []
        
        if result['rows']:
            # Convert column names to the expected format
            columns = [{"name": col, "type": "UNKNOWN"} for col in result['columns']]
            
            # Convert rows to dictionaries
            for row in result['rows']:
                row_dict = dict(zip(result['columns'], row))
                data.append(row_dict)
        
        # Return response
        return SqlPreviewResponse(
            data=data,
            row_count=len(data),
            total_row_count=None,  # We don't know the total without running a count query
            execution_time_ms=execution_time_ms,
            columns=columns,
            truncated=len(data) == request.limit
        )
    
    async def _validate_permissions(self, sources: List[SqlSource], user_id: int):
        """Validate read permissions."""
        checks = [
            PermissionCheck("dataset", source.dataset_id, user_id, "read")
            for source in sources
        ]
        await self._permissions.require_all(checks)