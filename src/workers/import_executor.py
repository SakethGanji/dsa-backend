"""Executor for import jobs with streaming and memory efficiency."""

import os
import json
import hashlib
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
from uuid import UUID

import aiofiles
from aiocsv import AsyncDictReader
import openpyxl
import pyarrow.parquet as pq

from src.workers.job_worker import JobExecutor
from src.infrastructure.postgres.database import DatabasePool
from src.infrastructure.config import get_settings


class ImportJobExecutor(JobExecutor):
    """Executes import jobs with optimized streaming pipeline."""
    
    def __init__(self):
        self.settings = get_settings()
        self.batch_size = self.settings.import_batch_size
        self.chunk_size = self.settings.import_chunk_size
    
    async def execute(self, job_id: str, parameters: Dict[str, Any], db_pool: DatabasePool) -> Dict[str, Any]:
        """Execute import job with streaming pipeline."""
        import logging
        logger = logging.getLogger(__name__)
        
        print(f"[IMPORT DEBUG] Starting job {job_id}")
        logger.info(f"Import job {job_id} - Starting optimized streaming import")
        
        # Handle case where parameters come as string
        if isinstance(parameters, str):
            parameters = json.loads(parameters)
        
        # Extract parameters
        temp_file_path = parameters['temp_file_path']
        filename = parameters['filename']
        commit_message = parameters['commit_message']
        target_ref = parameters['target_ref']
        dataset_id = parameters['dataset_id']
        user_id = parameters['user_id']
        file_size = os.path.getsize(temp_file_path) if os.path.exists(temp_file_path) else 0
        
        # Get job details from database
        async with db_pool.acquire() as conn:
            job = await conn.fetchrow(
                "SELECT source_commit_id FROM dsa_jobs.analysis_runs WHERE id = $1",
                UUID(job_id)
            )
            if not job:
                raise ValueError(f"Job with ID {job_id} not found.")
            parent_commit_id = job['source_commit_id']
        
        try:
            # Update job progress - starting
            await self._update_job_progress(job_id, {"status": "Parsing file", "percentage": 0}, db_pool)
            
            # Create commit first
            commit_id = await self._create_commit(
                db_pool, dataset_id, parent_commit_id, user_id, commit_message
            )
            
            # Determine file type and process with appropriate method
            file_ext = os.path.splitext(filename)[1].lower()
            
            if file_ext == '.csv':
                total_rows_processed = await self._process_csv_file(
                    commit_id, temp_file_path, job_id, file_size, db_pool
                )
            elif file_ext in ['.xlsx', '.xls']:
                total_rows_processed = await self._process_excel_file(
                    commit_id, temp_file_path, job_id, db_pool
                )
            elif file_ext == '.parquet':
                total_rows_processed = await self._process_parquet_file(
                    commit_id, temp_file_path, job_id, db_pool
                )
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
            
            # Update ref to point to new commit
            await self._update_ref(db_pool, dataset_id, target_ref, commit_id)
            
            # Analyze tables after import
            logger.info(f"Import job {job_id} - Analyzing imported tables")
            await self._analyze_imported_tables(commit_id, db_pool)
            
            # Refresh search index
            logger.info(f"Import job {job_id} - Refreshing search index")
            async with db_pool.acquire() as conn:
                await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_search.datasets_summary")
            
            # Clean up temp file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            
            # Update final job status
            await self._update_job_progress(
                job_id, 
                {"status": "Completed", "percentage": 100, "rows_processed": total_rows_processed}, 
                db_pool
            )
            
            return {
                "commit_id": commit_id,
                "rows_imported": total_rows_processed,
                "message": f"Successfully imported {total_rows_processed:,} rows"
            }
            
        except Exception as e:
            # Mark job as failed
            import traceback
            logger.error(f"Import job {job_id} failed: {str(e)}")
            logger.error(f"Full traceback:\n{traceback.format_exc()}")
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE dsa_jobs.analysis_runs 
                    SET status = 'failed', error_message = $2
                    WHERE id = $1
                """, UUID(job_id), str(e))
            
            # Clean up temp file on error
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            raise
    
    async def _process_and_commit_batch(
        self, 
        batch: List[Dict], 
        sheet_name: str, 
        start_row_idx: int, 
        commit_id: str, 
        db_pool: DatabasePool
    ) -> None:
        """
        Process and immediately commit a batch to the database.
        This is the key improvement - no accumulation in memory.
        """
        if not batch:
            return

        # Create temporary table name - use simple counter
        import random
        temp_table = f"temp_import_{random.randint(1000, 9999)}"
        
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Create temp table that auto-drops
                await conn.execute(f"""
                    CREATE TEMP TABLE {temp_table} (
                        logical_row_id TEXT,
                        row_hash VARCHAR(64),
                        data JSONB
                    ) ON COMMIT DROP
                """)
                
                # Prepare data for COPY
                copy_data = []
                for idx, row_data in enumerate(batch):
                    # TODO: [TECH DEBT - POC IMPLEMENTATION]
                    # This logical ID generation is based on a hash of the entire row's content.
                    #
                    # PROS: It correctly handles file re-sorting without creating a new commit.
                    # CONS: It CANNOT distinguish a row update from a DELETE and ADD operation.
                    #       This will cause significant performance degradation and database bloat
                    #       at scale when datasets are frequently updated.
                    #
                    # ACTION: This MUST be migrated to a Business Key-based hash generation
                    #         before moving to a production environment to enable true updates
                    #         and ensure system scalability. See ticket [POC-HASH-ID].
                    
                    # Calculate hash of data only (for both row_hash and logical_row_id)
                    data_json = json.dumps(row_data, sort_keys=True, separators=(',', ':'))
                    data_hash = hashlib.sha256(data_json.encode()).hexdigest()
                    
                    # Create logical row ID using the data hash
                    logical_row_id = f"{sheet_name}:{data_hash}"
                    
                    # Create standardized row format for storage
                    row = {
                        "sheet_name": sheet_name,
                        "row_number": start_row_idx + idx + 2,  # 1-indexed, +1 for header
                        "data": row_data
                    }
                    row_json = json.dumps(row, sort_keys=True)
                    
                    # Use the data hash as the row_hash (content-addressable storage)
                    row_hash = data_hash
                    
                    copy_data.append((logical_row_id, row_hash, row_json))
                
                # Use COPY to bulk insert into temp table
                await conn.copy_records_to_table(
                    temp_table,
                    records=copy_data,
                    columns=['logical_row_id', 'row_hash', 'data']
                )
                
                # Insert only new rows into dsa_core.rows
                await conn.execute(f"""
                    INSERT INTO dsa_core.rows (row_hash, data)
                    SELECT DISTINCT t.row_hash, t.data::jsonb
                    FROM {temp_table} t
                    LEFT JOIN dsa_core.rows r ON t.row_hash = r.row_hash
                    WHERE r.row_hash IS NULL
                """)
                
                # Insert commit_rows entries immediately - this is the key change
                await conn.execute(f"""
                    INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
                    SELECT $1, logical_row_id, row_hash FROM {temp_table}
                """, commit_id)
    
    async def _process_csv_file(
        self, 
        commit_id: str,
        file_path: str, 
        job_id: str, 
        file_size: int, 
        db_pool: DatabasePool
    ) -> int:
        """Process CSV file with true streaming - no memory accumulation."""
        sheet_name = os.path.splitext(os.path.basename(file_path))[0]
        total_rows_processed = 0
        
        async with aiofiles.open(file_path, mode='r', encoding='utf-8', newline='') as afp:
            reader = AsyncDictReader(afp)
            batch = []
            
            async for row_data in reader:
                batch.append(row_data)
                
                if len(batch) >= self.batch_size:
                    # Process and commit batch immediately
                    await self._process_and_commit_batch(
                        batch, sheet_name, total_rows_processed, commit_id, db_pool
                    )
                    total_rows_processed += len(batch)
                    batch = []
                    
                    # Update progress
                    try:
                        bytes_processed = await afp.tell()
                        percentage = (bytes_processed / file_size * 100) if file_size > 0 else 0
                    except:
                        # Fallback progress calculation
                        percentage = min((total_rows_processed / 100000 * 100), 99)
                    
                    await self._update_job_progress(
                        job_id,
                        {
                            "status": f"Processing: {total_rows_processed:,} rows",
                            "percentage": round(percentage, 2),
                            "rows_processed": total_rows_processed
                        },
                        db_pool
                    )
            
            # Process remaining rows
            if batch:
                await self._process_and_commit_batch(
                    batch, sheet_name, total_rows_processed, commit_id, db_pool
                )
                total_rows_processed += len(batch)
        
        return total_rows_processed
    
    async def _process_excel_file(
        self, 
        commit_id: str,
        file_path: str, 
        job_id: str, 
        db_pool: DatabasePool
    ) -> int:
        """Process Excel file with streaming - no memory accumulation."""
        # Open workbook in read-only mode for memory efficiency
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        
        total_rows_processed = 0
        
        for sheet_name in wb.sheetnames:
            sheet = wb[sheet_name]
            rows_iter = sheet.iter_rows(min_row=1)
            
            # Get headers
            try:
                headers = [cell.value for cell in next(rows_iter)]
            except StopIteration:
                continue  # Skip empty sheet
            
            batch = []
            sheet_rows_processed = 0
            
            for row in rows_iter:
                # Convert row to dictionary
                row_data = {
                    headers[i]: cell.value
                    for i, cell in enumerate(row)
                    if i < len(headers)
                }
                
                batch.append(row_data)
                
                if len(batch) >= self.batch_size:
                    # Process and commit batch immediately
                    await self._process_and_commit_batch(
                        batch, sheet_name, sheet_rows_processed, commit_id, db_pool
                    )
                    sheet_rows_processed += len(batch)
                    total_rows_processed += len(batch)
                    batch = []
                    
                    # Update progress
                    await self._update_job_progress(
                        job_id,
                        {
                            "status": f"Processing {sheet_name}: {sheet_rows_processed:,} rows",
                            "rows_processed": total_rows_processed
                        },
                        db_pool
                    )
            
            # Process remaining rows for this sheet
            if batch:
                await self._process_and_commit_batch(
                    batch, sheet_name, sheet_rows_processed, commit_id, db_pool
                )
                total_rows_processed += len(batch)
        
        wb.close()
        return total_rows_processed
    
    async def _process_parquet_file(
        self, 
        commit_id: str,
        file_path: str, 
        job_id: str, 
        db_pool: DatabasePool
    ) -> int:
        """Process Parquet file with streaming."""
        sheet_name = os.path.splitext(os.path.basename(file_path))[0]
        total_rows_processed = 0
        
        parquet_file = pq.ParquetFile(file_path)
        
        # Process file in batches
        for batch_data in parquet_file.iter_batches(batch_size=self.batch_size):
            batch = batch_data.to_pylist()
            
            await self._process_and_commit_batch(
                batch, sheet_name, total_rows_processed, commit_id, db_pool
            )
            total_rows_processed += len(batch)
            
            # Update progress
            await self._update_job_progress(
                job_id,
                {
                    "status": f"Processing: {total_rows_processed:,} rows",
                    "rows_processed": total_rows_processed
                },
                db_pool
            )
        
        return total_rows_processed
    
    async def _create_commit(
        self, db_pool: DatabasePool, dataset_id: int, parent_commit_id: Optional[str],
        user_id: int, message: str
    ) -> str:
        """Create a new commit."""
        import uuid
        # Generate a 64-character commit ID by padding with zeros
        timestamp_uuid = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}"
        commit_id = timestamp_uuid[:64].ljust(64, '0')
        
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO dsa_core.commits 
                    (commit_id, dataset_id, parent_commit_id, author_id, message,
                     authored_at, committed_at)
                VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
            """, commit_id, dataset_id, parent_commit_id, user_id, message)
        
        return commit_id
    
    async def _update_ref(
        self, db_pool: DatabasePool, dataset_id: int, ref_name: str, commit_id: str
    ) -> None:
        """Update reference to point to new commit."""
        async with db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE dsa_core.refs 
                SET commit_id = $1
                WHERE dataset_id = $2 AND name = $3
            """, commit_id, dataset_id, ref_name)
    
    async def _update_job_progress(
        self, job_id: str, progress_info: Dict[str, Any], db_pool: DatabasePool
    ) -> None:
        """Update job progress in analysis_runs table."""
        async with db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE dsa_jobs.analysis_runs 
                SET run_parameters = run_parameters || jsonb_build_object('progress', $1::jsonb)
                WHERE id = $2
            """, json.dumps(progress_info), UUID(job_id))
    
    async def _analyze_imported_tables(self, commit_id: str, db_pool: DatabasePool) -> None:
        """Analyze imported tables and store schema/statistics."""
        async with db_pool.acquire() as conn:
            # Get all unique table keys for this commit
            table_keys = await conn.fetch("""
                SELECT DISTINCT split_part(logical_row_id, ':', 1) as table_key
                FROM dsa_core.commit_rows
                WHERE commit_id = $1
            """, commit_id)
            
            for record in table_keys:
                table_key = record['table_key']
                
                # Get sample rows to analyze schema
                sample_rows = await conn.fetch("""
                    SELECT r.data
                    FROM dsa_core.commit_rows cr
                    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                    WHERE cr.commit_id = $1 
                    AND cr.logical_row_id LIKE $2 || ':%'
                    LIMIT 100
                """, commit_id, table_key)
                
                if not sample_rows:
                    continue
                
                # Analyze schema from sample
                all_columns = set()
                column_types = {}
                null_counts = {}
                unique_values = {}
                
                for row in sample_rows:
                    data = row['data']
                    
                    # Handle different data structures
                    if isinstance(data, str):
                        # Data is JSON string, need to parse it
                        data = json.loads(data)
                    
                    # All data follows the standardized format
                    if not isinstance(data, dict) or 'data' not in data:
                        raise ValueError(f"Invalid data format in analysis - expected standardized format")
                    
                    # Extract actual data from the standardized structure
                    row_data = data['data']
                    
                    # Skip if row_data is not a dict
                    if not isinstance(row_data, dict):
                        continue
                    
                    for col, value in row_data.items():
                        if col not in ['sheet_name', 'row_number']:
                            all_columns.add(col)
                            
                            # Track nulls
                            if col not in null_counts:
                                null_counts[col] = 0
                            if value is None:
                                null_counts[col] += 1
                            
                            # Track unique values
                            if col not in unique_values:
                                unique_values[col] = set()
                            if value is not None and len(unique_values[col]) < 100:
                                unique_values[col].add(str(value))
                            
                            # Infer type
                            if col not in column_types and value is not None:
                                if isinstance(value, bool):
                                    column_types[col] = 'boolean'
                                elif isinstance(value, int):
                                    column_types[col] = 'integer'
                                elif isinstance(value, float):
                                    column_types[col] = 'float'
                                else:
                                    column_types[col] = 'string'
                
                # Get total row count
                count_result = await conn.fetchrow("""
                    SELECT COUNT(*) as total
                    FROM dsa_core.commit_rows
                    WHERE commit_id = $1 AND logical_row_id LIKE $2 || ':%'
                """, commit_id, table_key)
                total_rows = count_result['total']
                
                # Build analysis structure
                analysis = {
                    'total_rows': total_rows,
                    'column_types': column_types,
                    'columns': sorted(list(all_columns)),
                    'null_counts': null_counts,
                    'unique_counts': {col: len(vals) for col, vals in unique_values.items()},
                    'statistics': {}
                }
                
                # Store analysis
                await conn.execute("""
                    INSERT INTO dsa_core.table_analysis (commit_id, table_key, analysis)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (commit_id, table_key) 
                    DO UPDATE SET analysis = $3, created_at = NOW()
                """, commit_id, table_key, json.dumps(analysis))
                
                # Also store schema in commit_schemas for compatibility
                schema_data = {
                    table_key: {
                        "columns": [{"name": col, "type": column_types.get(col, "string")} 
                                   for col in sorted(all_columns)]
                    }
                }
                
                # Check if schema already exists for this commit
                existing_schema = await conn.fetchval("""
                    SELECT schema_definition 
                    FROM dsa_core.commit_schemas 
                    WHERE commit_id = $1
                """, commit_id)
                
                if existing_schema:
                    # Update existing schema
                    if isinstance(existing_schema, str):
                        existing_schema = json.loads(existing_schema)
                    existing_schema.update(schema_data)
                    await conn.execute("""
                        UPDATE dsa_core.commit_schemas 
                        SET schema_definition = $2
                        WHERE commit_id = $1
                    """, commit_id, json.dumps(existing_schema))
                else:
                    # Insert new schema
                    await conn.execute("""
                        INSERT INTO dsa_core.commit_schemas (commit_id, schema_definition)
                        VALUES ($1, $2)
                    """, commit_id, json.dumps(schema_data))