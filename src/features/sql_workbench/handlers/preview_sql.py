"""Handler for previewing SQL query results."""
import time
from typing import List

from ...base_handler import BaseHandler, with_error_handling
from ....core.abstractions.uow import IUnitOfWork
from ....core.abstractions.service_interfaces import IWorkbenchService
from ....core.domain_exceptions import ForbiddenException
from ..models.sql_preview import SqlPreviewRequest, SqlPreviewResponse, SqlSource


class PreviewSqlHandler(BaseHandler[SqlPreviewResponse]):
    """Handler for executing SQL preview queries."""
    
    def __init__(self, uow: IUnitOfWork, workbench_service: IWorkbenchService):
        super().__init__(uow)
        self._workbench_service = workbench_service
    
    @with_error_handling
    async def handle(self, request: SqlPreviewRequest, user_id: int) -> SqlPreviewResponse:
        """Preview SQL query results."""
        # Validate permissions
        await self._validate_permissions(request.sources, user_id)
        
        # Get primary source and ref
        primary = request.sources[0]
        ref = await self._uow.commits.get_ref(primary.dataset_id, primary.ref)
        if not ref:
            raise ValueError(f"Ref '{primary.ref}' not found")
        
        # Build SQL with proper table references
        sql = request.sql
        for source in request.sources:
            sql = sql.replace(source.alias, source.table_key)
        
        # Execute preview
        start = time.time()
        result = await self._workbench_service.preview_transformation(
            dataset_id=primary.dataset_id,
            commit_id=ref['commit_id'],
            sql=sql,
            limit=request.limit
        )
        
        # Return response
        return SqlPreviewResponse(
            data=result.data,
            row_count=len(result.data),
            total_row_count=result.row_count,
            execution_time_ms=int((time.time() - start) * 1000),
            columns=[{"name": c["name"], "type": c["type"]} for c in result.schema.get("columns", [])],
            truncated=len(result.data) < result.row_count
        )
    
    async def _validate_permissions(self, sources: List[SqlSource], user_id: int):
        """Validate read permissions."""
        for source in sources:
            if not await self._uow.datasets.check_user_permission(
                source.dataset_id, user_id, 'read'
            ):
                raise ForbiddenException()