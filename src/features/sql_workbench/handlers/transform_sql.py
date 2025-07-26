"""Handler for SQL transformation jobs."""

from uuid import UUID
from typing import Dict, Any, List
from dataclasses import dataclass
from enum import Enum

from ...base_handler import BaseHandler, with_error_handling, with_transaction
from ....infrastructure.postgres.uow import PostgresUnitOfWork
from ....infrastructure.services.workbench_service import WorkbenchService
from ....infrastructure.postgres.dataset_repo import PostgresDatasetRepository
from ....infrastructure.postgres.job_repo import PostgresJobRepository
from ....infrastructure.postgres.versioning_repo import PostgresCommitRepository
# Use standard Python exceptions instead of custom error classes
from ..models.sql_transform import SqlTransformRequest, SqlTransformResponse
from src.core.domain_exceptions import ForbiddenException


# Data classes for workbench context
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


class TransformSqlHandler(BaseHandler[SqlTransformResponse]):
    """Handler for creating SQL transformation jobs."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        workbench_service: WorkbenchService,
        job_repository: PostgresJobRepository,
        dataset_repository: PostgresDatasetRepository,
        commit_repository: PostgresCommitRepository
    ):
        """Initialize handler with dependencies."""
        super().__init__(uow)
        self._workbench_service = workbench_service
        self._job_repository = job_repository
        self._dataset_repository = dataset_repository
        self._commit_repository = commit_repository
    
    @with_error_handling
    @with_transaction
    async def handle(
        self,
        request: SqlTransformRequest,
        user_id: int
    ) -> SqlTransformResponse:
        """
        Create a SQL transformation job.
        
        Args:
            request: SQL transformation request
            user_id: ID of the user creating the transformation
            
        Returns:
            SqlTransformResponse with job ID
        """
        # Validate permissions
        await self._validate_permissions(request, user_id)
        
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
        
        # Validate operation
        errors = await self._workbench_service.validate_operation(context, self._uow)
        if errors:
            raise ValueError(f"SQL transformation validation failed: {'; '.join(errors)}")
        
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
    
    async def _validate_permissions(self, request: SqlTransformRequest, user_id: int):
        """Validate user permissions for the transformation."""
        # Check read permissions for all sources
        for source in request.sources:
            has_read = await self._dataset_repository.check_user_permission(
                dataset_id=source.dataset_id,
                user_id=user_id,
                required_permission='read'
            )
            if not has_read:
                raise ForbiddenException()
        
        # Check write permission for target
        has_write = await self._dataset_repository.check_user_permission(
            dataset_id=request.target.dataset_id,
            user_id=user_id,
            required_permission='write'
        )
        if not has_write:
            raise ForbiddenException()
        
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