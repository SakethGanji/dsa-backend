"""Handler for retrieving sampling job data."""

from typing import Dict, Any, Optional, List
from uuid import UUID
import io
import csv

from src.core.abstractions.uow import IUnitOfWork
from src.core.abstractions.repositories import ITableReader
from ...base_handler import BaseHandler, with_error_handling
from src.core.common.pagination import PaginationMixin
from fastapi import HTTPException


class GetSamplingJobDataHandler(BaseHandler[Dict[str, Any]], PaginationMixin):
    """Handler for retrieving sampled data from a completed sampling job."""
    
    def __init__(self, uow: IUnitOfWork, table_reader: ITableReader):
        super().__init__(uow)
        self._table_reader = table_reader
    
    @with_error_handling
    async def handle(
        self,
        job_id: str,
        user_id: int,
        table_key: str = "primary",
        offset: int = 0,
        limit: int = 100,
        columns: Optional[List[str]] = None,
        format: str = "json"
    ) -> Dict[str, Any]:
        """
        Retrieve sampled data from a completed sampling job.
        
        Args:
            job_id: The sampling job ID
            user_id: Current user ID for permission checks
            table_key: Table to retrieve (default: "primary")
            offset: Pagination offset
            limit: Number of rows to return
            columns: Specific columns to include
            format: Output format ("json" or "csv")
            
        Returns:
            Paginated sampling data with metadata
        """
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
        has_permission = await self._uow.datasets.check_user_permission(
            dataset_id, user_id, "read"
        )
        
        if not has_permission:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")  # Don't reveal existence
        
        # Extract output commit ID from job output summary
        output_summary = job.get('output_summary', {})
        output_commit_id = output_summary.get('output_commit_id')
        
        if not output_commit_id:
            raise HTTPException(status_code=400, detail=f"Job {job_id} has no output commit")
        
        # Check if this is a residual data request
        is_residual = table_key == "residual"
        if is_residual:
            residual_commit_id = output_summary.get('residual_commit_id')
            if not residual_commit_id:
                raise HTTPException(status_code=400, detail=f"Job {job_id} has no residual data")
            output_commit_id = residual_commit_id
        
        # Get table row count
        total_rows = await self._table_reader.count_table_rows(
            output_commit_id, table_key if not is_residual else "primary"
        )
        
        if total_rows == 0:
            # Check if table exists by trying to get schema
            table_schema = await self._table_reader.get_table_schema(
                output_commit_id, table_key if not is_residual else "primary"
            )
            if not table_schema:
                raise HTTPException(status_code=404, detail=f"Table '{table_key}' not found in output")
        
        # For CSV format, return a streaming response
        if format == "csv":
            return await self._generate_csv_response(
                output_commit_id, table_key, columns, job_id
            )
        
        # Get paginated data
        table_data = await self._table_reader.get_table_data(
            output_commit_id,
            table_key if not is_residual else "primary",
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
            'table_key': table_key,
            'data': table_data,
            'pagination': {
                'total': total_rows,
                'offset': offset,
                'limit': limit,
                'has_more': offset + len(table_data) < total_rows
            },
            'metadata': {
                'sampling_summary': sampling_metadata,
                'is_residual': is_residual
            },
            'columns': list(table_data[0].keys()) if table_data else []
        }
        
        # Add round details if available
        if 'round_details' in sampling_metadata:
            response['metadata']['round_details'] = sampling_metadata['round_details']
        
        # Add residual info if this is the main sample
        if not is_residual and output_summary.get('residual_commit_id'):
            response['metadata']['residual_info'] = {
                'has_residual': True,
                'residual_count': output_summary.get('residual_count', 0),
                'residual_commit_id': output_summary['residual_commit_id']
            }
        
        return response
    
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