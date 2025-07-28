"""Consolidated service for all SQL workbench operations."""
import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.permissions import PermissionService, PermissionCheck
from src.services.workbench_service import WorkbenchService
from src.infrastructure.postgres.dataset_repo import PostgresDatasetRepository
from src.infrastructure.postgres.job_repo import PostgresJobRepository
from src.infrastructure.postgres.versioning_repo import PostgresCommitRepository
from ...base_handler import with_transaction, with_error_handling
from ..models import (
    SqlPreviewRequest, SqlPreviewResponse, SqlSource,
    SqlTransformRequest, SqlTransformResponse
)


class OperationType(Enum):
    """Types of operations supported by the workbench."""
    SQL_TRANSFORM = "sql_transform"
    PREVIEW = "preview"
    EXPORT = "export"


@dataclass
class WorkbenchContext:
    """Context for workbench operations."""
    user_id: int
    source_datasets: List[int]
    source_refs: List[str]
    operation_type: OperationType
    parameters: Dict[str, Any]


class SqlWorkbenchService:
    """Consolidated service for all SQL workbench operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        permissions: PermissionService,
        workbench_service: Optional[WorkbenchService] = None,
        job_repository: Optional[PostgresJobRepository] = None,
        dataset_repository: Optional[PostgresDatasetRepository] = None,
        commit_repository: Optional[PostgresCommitRepository] = None
    ):
        self._uow = uow
        self._permissions = permissions
        self._workbench = workbench_service or WorkbenchService()
        self._job_repository = job_repository
        self._dataset_repository = dataset_repository
        self._commit_repository = commit_repository
    
    @with_error_handling
    async def preview_sql(
        self,
        request: SqlPreviewRequest,
        user_id: int
    ) -> SqlPreviewResponse:
        """Preview SQL query results on sample data."""
        # Validate permissions
        await self._validate_preview_permissions(request.sources, user_id)
        
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
        if not self._workbench._db_pool:
            self._workbench._db_pool = self._uow._pool
        
        result = await self._workbench._execute_sql_with_sources(
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
    
    @with_transaction
    @with_error_handling
    async def transform_sql(
        self,
        request: SqlTransformRequest,
        user_id: int
    ) -> SqlTransformResponse:
        """Create SQL transformation job for asynchronous processing."""
        # Ensure repositories are available
        if not self._job_repository:
            raise ValueError("Job repository not provided")
        if not self._dataset_repository:
            raise ValueError("Dataset repository not provided")
        if not self._commit_repository:
            raise ValueError("Commit repository not provided")
        
        # Validate permissions
        await self._validate_transform_permissions(request, user_id)
        
        # Create workbench context
        context = WorkbenchContext(
            user_id=user_id,
            source_datasets=[s.dataset_id for s in request.sources],
            source_refs=[s.ref for s in request.sources],
            operation_type=OperationType.SQL_TRANSFORM,
            parameters={
                "sql": request.sql,
                "sources": [s.dict() for s in request.sources],
                "target": request.target.dict(),
                "dry_run": request.dry_run
            }
        )
        
        # Validate SQL syntax
        validation = await self._workbench.validate_transformation(request.sql)
        if not validation.is_valid:
            raise ValueError(f"SQL transformation validation failed: {'; '.join(validation.errors)}")
        
        # If dry run, return without creating job
        if request.dry_run:
            return SqlTransformResponse(
                job_id="dry-run-no-job-created",
                status="validated",
                estimated_rows=None
            )
        
        # Get current commit ID for the target ref
        target_dataset = await self._dataset_repository.get_dataset_by_id(request.target.dataset_id)
        if not target_dataset:
            raise KeyError(f"Target dataset {request.target.dataset_id} not found")
        
        # Get the current commit for the ref by querying refs directly
        refs = await self._commit_repository.list_refs(request.target.dataset_id)
        current_ref = next((r for r in refs if r['name'] == request.target.ref), None)
        if not current_ref:
            raise KeyError(f"Ref '{request.target.ref}' not found for dataset {request.target.dataset_id}")
        
        current_commit_id = current_ref['commit_id']
        
        # Create the job
        job_parameters = {
            "sources": [s.dict() for s in request.sources],
            "sql": request.sql,
            "target": request.target.dict(),
            "workbench_context": {
                "operation_type": context.operation_type.value,
                "user_id": user_id
            },
            "job_type": "sql_transform"  # Mark this as sql_transform for the executor
        }
        
        job_id = await self._job_repository.create_job(
            run_type="import",  # Using import type for now - would add sql_transform to DB enum
            dataset_id=request.target.dataset_id,
            user_id=user_id,
            source_commit_id=current_commit_id,
            run_parameters=job_parameters
        )
        
        # Estimate rows if possible (simplified - would use EXPLAIN in production)
        estimated_rows = await self._estimate_result_rows(request)
        
        return SqlTransformResponse(
            job_id=str(job_id),
            status="pending",
            estimated_rows=estimated_rows
        )
    
    async def _validate_preview_permissions(self, sources: List[SqlSource], user_id: int):
        """Validate read permissions for preview."""
        checks = [
            PermissionCheck("dataset", source.dataset_id, user_id, "read")
            for source in sources
        ]
        await self._permissions.require_all(checks)
    
    async def _validate_transform_permissions(self, request: SqlTransformRequest, user_id: int):
        """Validate user permissions for the transformation."""
        # Check read permissions for all sources
        checks = [
            PermissionCheck("dataset", source.dataset_id, user_id, "read")
            for source in request.sources
        ]
        await self._permissions.require_all(checks)
        
        # Check write permission for target
        await self._permissions.require("dataset", request.target.dataset_id, user_id, "write")
        
        # If creating new dataset, check create permission
        if request.target.create_new_dataset:
            # In this simplified version, assume user can create if they have write permission
            # In production, might have separate dataset creation permissions
            pass
    
    async def _estimate_result_rows(self, request: SqlTransformRequest) -> int:
        """Estimate the number of rows that will be produced by the transformation."""
        # Simplified estimation - in production would use EXPLAIN ANALYZE
        # For now, return None to indicate unknown
        return None