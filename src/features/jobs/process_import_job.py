import os
import hashlib
import json
from typing import Dict, Any, List, Tuple, Set
import pandas as pd

from core.services.interfaces import IUnitOfWork, IJobRepository, ICommitRepository
from uuid import UUID


class ProcessImportJobHandler:
    """Handler for processing import jobs in the background worker"""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        job_repo: IJobRepository,
        commit_repo: ICommitRepository
    ):
        self._uow = uow
        self._job_repo = job_repo
        self._commit_repo = commit_repo
    
    async def handle(self, job_id: UUID) -> None:
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
        # TODO: Get job details
        job = await self._job_repo.get_job_by_id(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        
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
            
            # TODO: Optimistic locking check
            current_commit = await self._commit_repo.get_current_commit_for_ref(
                job['dataset_id'], target_ref
            )
            
            if current_commit != job['source_commit_id']:
                raise Exception(f"Conflict: The '{target_ref}' branch has been updated. Please re-upload.")
            
            # TODO: Parse file
            rows_to_store, manifest, schema_def = await self._parse_file(
                temp_file_path, params['filename']
            )
            
            # TODO: Create commit in transaction
            await self._uow.begin()
            try:
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
                
                await self._uow.commit()
                
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
                await self._uow.rollback()
                raise
                
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
    
    async def _parse_file(
        self, 
        file_path: str, 
        filename: str
    ) -> Tuple[Set[Tuple[str, str]], List[Tuple[str, str]], Dict[str, Any]]:
        """
        Parse file and return (rows_to_store, manifest, schema_definition)
        """
        rows_to_store = set()
        manifest = []
        schema_def = {}
        
        # Determine file type
        ext = os.path.splitext(filename)[1].lower()
        
        if ext == '.csv':
            # Parse CSV
            df = pd.read_csv(file_path)
            sheets = {'default': df}
        elif ext == '.parquet':
            # Parse Parquet
            df = pd.read_parquet(file_path)
            sheets = {'default': df}
        elif ext in ['.xlsx', '.xls']:
            # Parse Excel with multiple sheets
            sheets = pd.read_excel(file_path, sheet_name=None)
        else:
            raise ValueError(f"Unsupported file type: {ext}")
        
        # Process each sheet
        for sheet_name, df in sheets.items():
            # Infer schema
            sheet_schema = {
                'columns': {},
                'row_count': len(df)
            }
            
            for col in df.columns:
                dtype = str(df[col].dtype)
                sheet_schema['columns'][col] = {
                    'type': self._map_dtype_to_type(dtype),
                    'nullable': df[col].isnull().any()
                }
            
            schema_def[sheet_name] = sheet_schema
            
            # Process rows
            for idx, row in df.iterrows():
                logical_row_id = f"{sheet_name}:{idx}"
                row_data = row.to_dict()
                
                # Handle NaN values
                row_data = {k: None if pd.isna(v) else v for k, v in row_data.items()}
                
                # Canonicalize and hash
                canonical_json = json.dumps(row_data, sort_keys=True, separators=(',', ':'))
                row_hash = hashlib.sha256(canonical_json.encode()).hexdigest()
                
                rows_to_store.add((row_hash, canonical_json))
                manifest.append((logical_row_id, row_hash))
        
        return rows_to_store, manifest, schema_def
    
    def _map_dtype_to_type(self, dtype: str) -> str:
        """Map pandas dtype to our type system"""
        if 'int' in dtype:
            return 'integer'
        elif 'float' in dtype:
            return 'number'
        elif 'bool' in dtype:
            return 'boolean'
        elif 'datetime' in dtype:
            return 'datetime'
        else:
            return 'string'