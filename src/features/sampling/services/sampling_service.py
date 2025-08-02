"""Consolidated service for all sampling operations."""

from typing import Dict, Any, List, Optional, Tuple
from uuid import UUID
from datetime import datetime
from dataclasses import dataclass
import io
import csv

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.table_reader import PostgresTableReader
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus
from src.core.domain_exceptions import EntityNotFoundException, ValidationException
from ...base_handler import with_transaction, with_error_handling
from fastapi import HTTPException
from ..models import CreateSamplingJobCommand, GetSamplingMethodsCommand


@dataclass
class SamplingJobResponse:
    job_id: str
    status: str
    message: str


class SamplingService:
    """Consolidated service for all sampling operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        permissions: PermissionService,
        table_reader: Optional[PostgresTableReader] = None,
        event_bus: Optional[EventBus] = None
    ):
        self._uow = uow
        self._permissions = permissions
        self._table_reader = table_reader
        self._event_bus = event_bus
    
    @with_transaction
    @with_error_handling
    async def create_sampling_job(
        self,
        command: CreateSamplingJobCommand
    ) -> SamplingJobResponse:
        """Create a sampling job for asynchronous processing."""
        # Check permissions - write permission needed to create sampling job
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "write")
        
        async with self._uow:
            # Get current commit for ref
            ref = await self._uow.commits.get_ref(command.dataset_id, command.source_ref)
            if not ref:
                raise EntityNotFoundException("Ref", command.source_ref)
            
            source_commit_id = ref['commit_id']
            
            # Build job parameters
            job_params = {
                'source_commit_id': source_commit_id,
                'dataset_id': command.dataset_id,
                'table_key': command.table_key,
                'create_output_commit': True,  # Always create output commit
                'output_branch_name': command.output_branch_name,
                'output_name': command.output_name,
                'commit_message': command.commit_message,
                'user_id': command.user_id,
                'rounds': command.rounds,
                'export_residual': command.export_residual,
                'residual_output_name': command.residual_output_name
            }
            
            # Create job directly in the database
            job_id = await self._uow.jobs.create_job(
                run_type='sampling',
                dataset_id=command.dataset_id,
                source_commit_id=source_commit_id,
                user_id=command.user_id,
                run_parameters=job_params  # Pass as dict, not JSON string
            )
            
            await self._uow.commit()
            
            return SamplingJobResponse(
                job_id=str(job_id),
                status="pending",
                message=f"Sampling job created with {len(command.rounds)} rounds"
            )
    
    @with_error_handling
    async def get_job_data(
        self,
        job_id: str,
        user_id: int,
        table_key: str = "primary",
        offset: int = 0,
        limit: int = 100,
        columns: Optional[List[str]] = None,
        format: str = "json"
    ) -> Dict[str, Any]:
        """Retrieve sampled data from a completed sampling job."""
        # Validate format
        if format not in ["json", "csv"]:
            raise HTTPException(status_code=400, detail=f"Invalid format: {format}. Must be 'json' or 'csv'")
        
        # Get job details
        job_uuid = UUID(job_id)
        job = await self._uow.jobs.get_job_by_id(job_uuid)
        
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        # Check job type and status
        if job.get('run_type') != 'sampling':
            raise HTTPException(status_code=400, detail=f"Job {job_id} is not a sampling job")
        
        if job.get('status') != 'completed':
            raise HTTPException(status_code=400, detail=f"Job {job_id} is not completed (status: {job.get('status')})")
        
        # Check user permissions on the dataset
        dataset_id = job.get('dataset_id')
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Extract output commit ID from job output summary
        output_summary = job.get('output_summary', {})
        output_commit_id = output_summary.get('output_commit_id')
        
        if not output_commit_id:
            raise HTTPException(status_code=400, detail=f"Job {job_id} has no output commit")
        
        # With the new structure, we use fixed table keys: 'sample' and 'residual'
        # Map the requested table_key parameter to the actual table key
        if table_key in ["primary", "sample"]:
            actual_table_key = "sample"
            is_residual = False
        elif table_key == "residual":
            actual_table_key = "residual"
            is_residual = True
            # Check if residual data exists
            residual_count = output_summary.get('residual_count', 0)
            if residual_count == 0:
                raise HTTPException(status_code=400, detail=f"Job {job_id} has no residual data")
        else:
            # For backward compatibility, default to 'sample'
            actual_table_key = "sample"
            is_residual = False
        
        if not self._table_reader:
            raise HTTPException(status_code=500, detail="Table reader not available")
        
        # Get table row count using the actual table key
        total_rows = await self._table_reader.count_table_rows(
            output_commit_id, actual_table_key
        )
        
        if total_rows == 0:
            # Check if table exists by trying to get schema
            table_schema = await self._table_reader.get_table_schema(
                output_commit_id, actual_table_key
            )
            if not table_schema:
                raise HTTPException(status_code=404, detail=f"Table '{actual_table_key}' not found in output")
        
        # For CSV format, return a streaming response
        if format == "csv":
            return await self._generate_csv_response(
                output_commit_id, actual_table_key, columns, job_id
            )
        
        # Get paginated data
        table_data = await self._table_reader.get_table_data(
            output_commit_id,
            actual_table_key,
            offset=offset,
            limit=limit
        )
        
        # Filter columns if requested
        if columns:
            table_data = [
                {k: v for k, v in row.items() if k in columns or k == '_logical_row_id'}
                for row in table_data
            ]
        
        # Get sampling metadata
        sampling_metadata = output_summary.get('sampling_summary', {})
        
        # Build response
        response = {
            'job_id': job_id,
            'dataset_id': dataset_id,
            'commit_id': output_commit_id,
            'table_key': actual_table_key,
            'data': table_data,
            'pagination': {
                'total': total_rows,
                'offset': offset,
                'limit': limit,
                'has_more': offset + len(table_data) < total_rows
            },
            'metadata': {
                'sampling_summary': sampling_metadata,
                'is_residual': is_residual,
                'original_table_key': table_key  # Include the requested table key for reference
            },
            'columns': list(table_data[0].keys()) if table_data else []
        }
        
        # Add round details if available
        if 'round_details' in sampling_metadata:
            response['metadata']['round_details'] = sampling_metadata['round_details']
        
        # Add residual info if this is the main sample
        if not is_residual and output_summary.get('residual_count', 0) > 0:
            response['metadata']['residual_info'] = {
                'has_residual': True,
                'residual_count': output_summary.get('residual_count', 0),
                'table_key': 'residual'  # Now in the same commit
            }
        
        return response
    
    @with_error_handling
    async def get_dataset_sampling_history(
        self,
        dataset_id: int,
        user_id: int,
        ref_name: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Dict[str, Any]:
        """Get sampling job history for a dataset."""
        # Check dataset exists and user has permission
        dataset = await self._uow.datasets.get_dataset_by_id(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
        
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Get sampling jobs
        jobs, total_count = await self._uow.jobs.get_sampling_jobs_by_dataset(
            dataset_id=dataset_id,
            ref_name=ref_name,
            status=status,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=limit
        )
        
        # Build response
        return {
            'dataset_id': dataset_id,
            'dataset_name': dataset['name'],
            'jobs': jobs,
            'pagination': {
                'total': total_count,
                'offset': offset,
                'limit': limit,
                'has_more': offset + len(jobs) < total_count
            }
        }
    
    @with_error_handling
    async def get_user_sampling_history(
        self,
        target_user_id: int,
        current_user_id: int,
        is_admin: bool = False,
        dataset_id: Optional[int] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Dict[str, Any]:
        """Get sampling job history for a user."""
        # Check permissions - users can only see their own history unless admin
        if target_user_id != current_user_id and not is_admin:
            raise HTTPException(status_code=404, detail=f"User {target_user_id} not found")
        
        # Get user info
        user = await self._uow.users.get_by_id(target_user_id)
        if not user:
            raise HTTPException(status_code=404, detail=f"User {target_user_id} not found")
        
        # Get sampling jobs
        jobs, total_count = await self._uow.jobs.get_sampling_jobs_by_user(
            user_id=target_user_id,
            dataset_id=dataset_id,
            status=status,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=limit
        )
        
        # Calculate summary statistics
        summary = await self._calculate_user_summary(target_user_id)
        
        # Build response
        return {
            'user_id': target_user_id,
            'user_soeid': user.get('soeid'),
            'user_name': user.get('soeid'),  # Using soeid as name since first_name/last_name don't exist
            'jobs': jobs,
            'pagination': {
                'total': total_count,
                'offset': offset,
                'limit': limit,
                'has_more': offset + len(jobs) < total_count
            },
            'summary': summary
        }
    
    @with_error_handling
    async def get_sampling_methods(
        self,
        dataset_id: int,
        user_id: int
    ) -> Dict[str, Any]:
        """Get available sampling methods and their parameters."""
        # Check permissions - read permission needed
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Check dataset exists
        dataset = await self._uow.datasets.get_dataset_by_id(dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", dataset_id)
        
        # Get available methods
        from ..models.sampling import SamplingMethod
        methods = list(SamplingMethod)
        
        return {
            "methods": [
                {
                    "name": method.value,
                    "description": self._get_method_description(method),
                    "parameters": self._get_method_parameters(method)
                }
                for method in methods
            ],
            "supported_operators": [
                ">", ">=", "<", "<=", "=", "!=", "in", "not_in", 
                "like", "ilike", "is_null", "is_not_null"
            ]
        }
    
    async def _generate_csv_response(
        self,
        commit_id: str,
        table_key: str,
        columns: Optional[List[str]],
        job_id: str
    ) -> Dict[str, Any]:
        """Generate CSV response for download."""
        # For CSV, we'll return metadata that the API layer can use
        # to stream the response
        return {
            'format': 'csv',
            'commit_id': commit_id,
            'table_key': table_key,
            'columns': columns,
            'filename': f'sampling_job_{job_id}_export.csv',
            '_stream_response': True  # Flag for API layer
        }
    
    async def _calculate_user_summary(self, user_id: int) -> Dict[str, Any]:
        """Calculate summary statistics for user's sampling jobs."""
        # Get all user's sampling jobs for summary (without pagination)
        all_jobs, total = await self._uow.jobs.get_sampling_jobs_by_user(
            user_id=user_id,
            offset=0,
            limit=1000  # Reasonable limit for summary calculation
        )
        
        # Calculate statistics
        successful_jobs = sum(1 for job in all_jobs if job['status'] == 'completed')
        failed_jobs = sum(1 for job in all_jobs if job['status'] == 'failed')
        total_rows_sampled = 0
        datasets_sampled = set()
        
        for job in all_jobs:
            if job['status'] == 'completed' and job.get('sampling_summary'):
                total_rows_sampled += job['sampling_summary'].get('total_samples', 0)
            datasets_sampled.add(job['dataset_id'])
        
        return {
            'total_sampling_jobs': total,
            'successful_jobs': successful_jobs,
            'failed_jobs': failed_jobs,
            'total_rows_sampled': total_rows_sampled,
            'datasets_sampled': len(datasets_sampled)
        }
    
    def _get_method_description(self, method) -> str:
        """Get description for sampling method."""
        # Import here to avoid circular dependency
        from ..models.sampling import SamplingMethod
        
        descriptions = {
            SamplingMethod.RANDOM: "Simple random sampling with optional seed for reproducibility",
            SamplingMethod.STRATIFIED: "Stratified sampling ensuring representation from all strata",
            SamplingMethod.SYSTEMATIC: "Systematic sampling with fixed intervals",
            SamplingMethod.CLUSTER: "Cluster sampling selecting entire groups",
            SamplingMethod.RESERVOIR: "Reservoir sampling for memory-efficient sampling"
        }
        return descriptions.get(method, "")
    
    def _get_method_parameters(self, method) -> List[Dict[str, Any]]:
        """Get required and optional parameters for each method."""
        # Import here to avoid circular dependency
        from ..models.sampling import SamplingMethod
        
        base_params = [
            {"name": "sample_size", "type": "integer", "required": True, "description": "Number of samples"},
            {"name": "seed", "type": "integer", "required": False, "description": "Random seed"}
        ]
        
        method_specific = {
            SamplingMethod.STRATIFIED: [
                {"name": "strata_columns", "type": "array", "required": True, "description": "Columns to stratify by"},
                {"name": "min_per_stratum", "type": "integer", "required": False, "description": "Minimum samples per stratum"},
                {"name": "proportional", "type": "boolean", "required": False, "description": "Use proportional allocation"}
            ],
            SamplingMethod.CLUSTER: [
                {"name": "cluster_column", "type": "string", "required": True, "description": "Column defining clusters"},
                {"name": "num_clusters", "type": "integer", "required": True, "description": "Number of clusters to select"},
                {"name": "samples_per_cluster", "type": "integer", "required": False, "description": "Samples per cluster"}
            ],
            SamplingMethod.SYSTEMATIC: [
                {"name": "interval", "type": "integer", "required": True, "description": "Sampling interval"},
                {"name": "start", "type": "integer", "required": False, "description": "Starting position"}
            ]
        }
        
        return base_params + method_specific.get(method, [])