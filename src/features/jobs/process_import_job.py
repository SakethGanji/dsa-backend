import os
import hashlib
import json
from typing import Dict, Any, List, Tuple, Set
import pandas as pd

from src.core.abstractions import IUnitOfWork, IJobRepository, ICommitRepository
from src.core.abstractions.services import IFileProcessingService, IStatisticsService
from src.features.base_handler import BaseHandler, with_error_handling
from src.core.decorators import requires_permission
from uuid import UUID
from dataclasses import dataclass
from src.core.domain_exceptions import EntityNotFoundException


@dataclass
class ProcessImportJobCommand:
    user_id: int  # Must be first for decorator
    job_id: UUID
    dataset_id: int  # For permission check


class ProcessImportJobHandler(BaseHandler):
    """Handler for processing import jobs in the background worker"""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        job_repo: IJobRepository,
        commit_repo: ICommitRepository,
        parser_factory: IFileProcessingService,
        stats_calculator: IStatisticsService
    ):
        super().__init__(uow)
        self._job_repo = job_repo
        self._commit_repo = commit_repo
        self._parser_factory = parser_factory
        self._stats_calculator = stats_calculator
    
    @with_error_handling
    @requires_permission("dataset", "write")
    async def handle(self, user_id: int, job_id: UUID, dataset_id: int) -> None:
        """
        Process an import job
        
        Steps:
        1. Get job details
        2. Check optimistic locking (source commit)
        3. Parse file and generate rows
        4. Create commit atomically
        5. Update job status
        6. Clean up temp file
        """
        # Get job details
        job = await self._job_repo.get_job_by_id(job_id)
        if not job:
            raise EntityNotFoundException("Job", job_id)
        
        if job['status'] != 'pending':
            return  # Already processed
        
        try:
            # Mark job as running
            await self._job_repo.update_job_status(job_id, 'running')
            
            # Extract parameters
            params = job['run_parameters']
            temp_file_path = params['temp_file_path']
            target_ref = params['target_ref']
            commit_message = params['commit_message']
            
            # Optimistic locking check
            current_commit = await self._commit_repo.get_current_commit_for_ref(
                job['dataset_id'], target_ref
            )
            
            if current_commit != job['source_commit_id']:
                raise Exception(f"Conflict: The '{target_ref}' branch has been updated. Please re-upload.")
            
            # Parse file using abstracted parser
            rows_to_store, manifest, schema_def, table_statistics = await self._parse_file(
                temp_file_path, params['filename']
            )
            
            # Create commit in transaction
            async with self._create_transaction():
                # Add unique rows
                await self._commit_repo.add_rows_if_not_exist(rows_to_store)
                
                # Create commit with manifest
                new_commit_id = await self._commit_repo.create_commit_and_manifest(
                    dataset_id=job['dataset_id'],
                    parent_commit_id=job['source_commit_id'],
                    message=commit_message,
                    author_id=job['user_id'],
                    manifest=manifest
                )
                
                # Update ref
                success = await self._commit_repo.update_ref_atomically(
                    dataset_id=job['dataset_id'],
                    ref_name=target_ref,
                    new_commit_id=new_commit_id,
                    expected_commit_id=job['source_commit_id']
                )
                
                if not success:
                    raise Exception("Failed to update ref - concurrent modification")
                
                # Store schema
                await self._commit_repo.create_commit_schema(new_commit_id, schema_def)
                
                # Note: Statistics are now stored in table_analysis during import
                
                # Update job as completed
                output_summary = {
                    "new_commit_id": new_commit_id,
                    "updated_ref": target_ref,
                    "rows_processed": len(manifest),
                    "sheets": list(schema_def.keys())
                }
                
                await self._job_repo.update_job_status(
                    job_id, 'completed', output_summary=output_summary
                )
                
        except Exception as e:
            # Mark job as failed
            await self._job_repo.update_job_status(
                job_id, 'failed', error_message=str(e)
            )
            raise
        finally:
            # Clean up temp file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
    
    async def _create_transaction(self):
        """Context manager for manual transaction handling in complex operations"""
        await self._uow.begin()
        try:
            yield
            await self._uow.commit()
        except Exception:
            await self._uow.rollback()
            raise
    
    async def _parse_file(
        self, 
        file_path: str, 
        filename: str
    ) -> Tuple[Set[Tuple[str, str]], List[Tuple[str, str]], Dict[str, Any], Dict[str, Any]]:
        """
        Parse file and return processed data.
        
        Returns:
            rows_to_store: Set of (row_hash, json_data) tuples
            manifest: List of (logical_row_id, row_hash) tuples
            schema_definition: Dict mapping table_key to schema info
            statistics: Dict mapping table_key to statistics
        """
        rows_to_store = set()
        manifest = []
        schema_def = {}
        statistics = {}
        
        # Use parser factory to get appropriate parser
        parser = self._parser_factory.get_parser(filename)
        parsed_data = await parser.parse(file_path, filename)
        
        # Process each table
        for table_data in parsed_data.tables:
            table_key = table_data.table_key
            df = table_data.dataframe
            
            # Calculate statistics for this table
            table_stats = await self._stats_calculator.calculate_table_statistics(
                df, table_key
            )
            
            # Convert statistics to storage format
            statistics[table_key] = self._stats_calculator.get_summary_dict(table_stats)
            
            # Build schema from statistics
            sheet_schema = {
                'columns': {},
                'row_count': table_stats.row_count
            }
            
            for col_name, col_stats in table_stats.columns.items():
                sheet_schema['columns'][col_name] = {
                    'type': col_stats.dtype,
                    'nullable': col_stats.null_count > 0
                }
            
            schema_def[table_key] = sheet_schema
            
            # Process rows
            for idx, row in df.iterrows():
                logical_row_id = f"{table_key}:{idx}"
                row_data = row.to_dict()
                
                # Handle NaN values
                row_data = {k: None if pd.isna(v) else v for k, v in row_data.items()}
                
                # Canonicalize and hash
                canonical_json = json.dumps(row_data, sort_keys=True, separators=(',', ':'))
                row_hash = hashlib.sha256(canonical_json.encode()).hexdigest()
                
                rows_to_store.add((row_hash, canonical_json))
                manifest.append((logical_row_id, row_hash))
        
        return rows_to_store, manifest, schema_def, statistics