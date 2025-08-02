"""Simplified import executor that only processes Parquet files."""

import os
import json
import asyncio
from typing import Dict, Any, List, Tuple, Optional
from uuid import UUID
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import io
import tempfile

import aiofiles
import aiofiles.os
import polars as pl
import pyarrow.parquet as pq
import xxhash
import psycopg

from src.workers.job_worker import JobExecutor
from src.workers.file_converter import FileConverter
from src.infrastructure.postgres.database import DatabasePool
from src.infrastructure.postgres.event_store import PostgresEventStore
from src.core.events.publisher import JobStartedEvent, JobCompletedEvent, JobFailedEvent
from src.core.events.registry import InMemoryEventBus
from src.infrastructure.config import get_settings


class ImportJobExecutor(JobExecutor):
    """Executes import jobs using standardized Parquet format."""
    
    def __init__(self):
        self.settings = get_settings()
        self.batch_size = self.settings.import_batch_size
        self.use_xxhash = True
        self.xxhash_seed = 0
        self.parallel_workers = self.settings.import_parallel_workers
        self.parallel_threshold_mb = self.settings.import_parallel_threshold_mb
        self.db_url = f"postgresql://{self.settings.POSTGRESQL_USER}:{self.settings.POSTGRESQL_PASSWORD}@{self.settings.POSTGRESQL_HOST}:{self.settings.POSTGRESQL_PORT}/{self.settings.POSTGRESQL_DATABASE}"
        self.file_converter = FileConverter()
    
    async def execute(self, job_id: str, parameters: Dict[str, Any], db_pool: DatabasePool) -> Dict[str, Any]:
        """Execute import job with file conversion and parallel Parquet processing."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Import job {job_id} - Starting import with conversion pipeline")
        
        # Create event bus
        event_store = PostgresEventStore(db_pool)
        event_bus = InMemoryEventBus()
        event_bus.set_event_store(event_store)
        
        # Handle string parameters
        if isinstance(parameters, str):
            parameters = json.loads(parameters)
        
        # Extract parameters
        temp_file_path = parameters.get('temp_file_path', parameters.get('file_path'))
        filename = parameters.get('filename', parameters.get('file_name'))
        
        if not await aiofiles.os.path.exists(temp_file_path):
            raise FileNotFoundError(f"Import file not found: {temp_file_path}")
        
        # Ensure filename is available for proper extension detection
        if not filename:
            raise ValueError("Filename is required for file type detection")
        
        # Get job details
        async with db_pool.acquire() as conn:
            job = await conn.fetchrow(
                "SELECT source_commit_id, dataset_id, user_id FROM dsa_jobs.analysis_runs WHERE id = $1",
                UUID(job_id)
            )
            if not job:
                raise ValueError(f"Job {job_id} not found")
            
            parent_commit_id = job['source_commit_id']
            dataset_id = parameters.get('dataset_id', job['dataset_id'])
            user_id = parameters.get('user_id', job['user_id'])
        
        # Create temporary directory for conversion
        temp_dir = tempfile.mkdtemp(prefix='dsa_import_')
        
        try:
            # Publish job started event
            await event_bus.publish(JobStartedEvent(
                job_id=job_id,
                job_type='import',
                dataset_id=dataset_id,
                user_id=user_id
            ))
            
            # Update progress - starting
            await self._update_job_progress(job_id, {
                "status": "Converting file to Parquet format",
                "percentage": 5
            }, db_pool)
            
            # Phase 1: Convert file to Parquet
            logger.info(f"Import job {job_id} - Converting {filename} to Parquet")
            converted_files, conversion_metadata = await self.file_converter.convert_to_parquet(
                source_path=temp_file_path,
                output_dir=temp_dir,
                original_filename=filename
            )
            
            logger.info(f"Import job {job_id} - Conversion complete. Created {len(converted_files)} Parquet files")
            
            # Store conversion metadata in job parameters
            await self._store_conversion_metadata(job_id, conversion_metadata, db_pool)
            
            # Update progress - conversion complete
            await self._update_job_progress(job_id, {
                "status": "Creating commit",
                "percentage": 20,
                "conversion_metadata": conversion_metadata
            }, db_pool)
            
            # Create commit
            commit_id = await self._create_commit(
                db_pool, dataset_id, parent_commit_id, user_id, 
                parameters.get('commit_message', f"Import {filename}")
            )
            
            # Phase 2: Import all Parquet files
            total_rows_processed = 0
            
            for idx, (table_key, parquet_path) in enumerate(converted_files):
                table_progress = 20 + (idx * 70 // len(converted_files))
                
                await self._update_job_progress(job_id, {
                    "status": f"Importing table '{table_key}'",
                    "percentage": table_progress,
                    "current_table": table_key,
                    "tables_completed": idx,
                    "total_tables": len(converted_files)
                }, db_pool)
                
                logger.info(f"Import job {job_id} - Importing table '{table_key}' from {parquet_path}")
                
                # Process this Parquet file
                rows_processed = await self._process_parquet_file(
                    commit_id=commit_id,
                    file_path=parquet_path,
                    table_key=table_key,
                    job_id=job_id,
                    db_pool=db_pool
                )
                
                total_rows_processed += rows_processed
                logger.info(f"Import job {job_id} - Table '{table_key}' imported {rows_processed:,} rows")
            
            # Update ref
            await self._update_ref(
                db_pool, dataset_id, 
                parameters.get('target_ref', 'main'), 
                commit_id
            )
            
            # Run post-import maintenance
            await self._update_job_progress(job_id, {
                "status": "Running post-import optimization",
                "percentage": 95
            }, db_pool)
            
            await self._run_post_import_maintenance(commit_id, job_id, db_pool)
            
            # Final update
            await self._update_job_progress(job_id, {
                "status": "Completed",
                "percentage": 100,
                "rows_processed": total_rows_processed,
                "tables_imported": len(converted_files)
            }, db_pool)
            
            # Publish completion event
            await event_bus.publish(JobCompletedEvent(
                job_id=job_id,
                status='completed',
                dataset_id=dataset_id,
                result={
                    "commit_id": commit_id,
                    "rows_imported": total_rows_processed,
                    "tables_imported": len(converted_files),
                    "conversion_metadata": conversion_metadata
                }
            ))
            
            return {
                "commit_id": commit_id,
                "rows_imported": total_rows_processed,
                "tables_imported": len(converted_files),
                "message": f"Successfully imported {total_rows_processed:,} rows from {len(converted_files)} table(s)"
            }
            
        except Exception as e:
            import traceback
            logger.error(f"Import job {job_id} failed: {e}\n{traceback.format_exc()}")
            
            await event_bus.publish(JobFailedEvent(
                job_id=job_id,
                error_message=str(e),
                dataset_id=dataset_id
            ))
            
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE dsa_jobs.analysis_runs SET status = 'failed', error_message = $2 WHERE id = $1",
                    UUID(job_id), str(e)
                )
            raise
            
        finally:
            # Cleanup
            if await aiofiles.os.path.exists(temp_file_path):
                await aiofiles.os.remove(temp_file_path)
            
            # Clean up conversion directory
            if os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir)
                logger.info(f"Import job {job_id} - Cleaned up temporary files")
    
    def _calculate_hash(self, data: bytes) -> str:
        """Calculate hash using xxHash for performance."""
        if self.use_xxhash:
            return xxhash.xxh64(data, seed=self.xxhash_seed).hexdigest()
        else:
            import hashlib
            return hashlib.sha256(data).hexdigest()
    
    def _convert_datetimes(self, obj: Any) -> Any:
        """Recursively convert datetime objects to ISO format strings."""
        from datetime import datetime, date
        
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self._convert_datetimes(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_datetimes(item) for item in obj]
        else:
            return obj
    
    async def _process_parquet_file(
        self,
        commit_id: str,
        file_path: str,
        table_key: str,
        job_id: str,
        db_pool: DatabasePool
    ) -> int:
        """Process a single Parquet file, using parallel processing for large files."""
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        if file_size_mb > self.parallel_threshold_mb and self.parallel_workers > 1:
            return await self._process_parquet_parallel(
                commit_id, file_path, table_key, job_id, db_pool
            )
        else:
            return await self._process_parquet_sequential(
                commit_id, file_path, table_key, job_id, db_pool
            )
    
    async def _process_parquet_sequential(
        self,
        commit_id: str,
        file_path: str,
        table_key: str,
        job_id: str,
        db_pool: DatabasePool
    ) -> int:
        """Process Parquet file sequentially for smaller files."""
        # Run blocking reader in thread pool
        loop = asyncio.get_running_loop()
        batches = await loop.run_in_executor(
            None, self._read_parquet_batches, file_path
        )
        
        total_rows = 0
        
        for batch_df in batches:
            # Calculate line numbers for this batch
            start_line = total_rows + 2  # Line numbers start at 2 (after header)
            
            # Create batch with proper line numbers
            batch_rows = [
                (start_line + i, row) 
                for i, row in enumerate(batch_df.iter_rows(named=True))
            ]
            
            # Commit batch
            await self._commit_batch(batch_rows, table_key, commit_id, db_pool)
            total_rows += len(batch_rows)
            
            # Update progress periodically
            if total_rows % (self.batch_size * 10) == 0:
                await self._update_job_progress(job_id, {
                    "rows_processed": total_rows,
                    "current_table": table_key
                }, db_pool)
        
        return total_rows
    
    def _read_parquet_batches(self, file_path: str) -> List[pl.DataFrame]:
        """Read Parquet file in batches (synchronous)."""
        parquet_file = pq.ParquetFile(file_path)
        batches = []
        
        for batch in parquet_file.iter_batches(batch_size=self.batch_size):
            batches.append(pl.from_arrow(batch))
        
        return batches
    
    async def _process_parquet_parallel(
        self,
        commit_id: str,
        file_path: str,
        table_key: str,
        job_id: str,
        db_pool: DatabasePool
    ) -> int:
        """Process large Parquet files in parallel."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Import job {job_id} - Starting parallel processing with {self.parallel_workers} workers")
        
        # Get file info
        loop = asyncio.get_running_loop()
        
        def get_file_info():
            pf = pq.ParquetFile(file_path)
            return pf.metadata.num_rows, len(pf.metadata.row_groups)
        
        total_rows, num_row_groups = await loop.run_in_executor(None, get_file_info)
        
        # Distribute row groups among workers
        row_groups_per_worker = max(1, num_row_groups // self.parallel_workers)
        worker_assignments = []
        
        for i in range(self.parallel_workers):
            start_group = i * row_groups_per_worker
            if i == self.parallel_workers - 1:
                # Last worker takes remaining groups
                end_group = num_row_groups
            else:
                end_group = (i + 1) * row_groups_per_worker
            
            if start_group < num_row_groups:
                worker_assignments.append((start_group, end_group))
        
        # Progress tracking
        mp_manager = mp.Manager()
        progress_queue = mp_manager.Queue()
        
        async def progress_listener():
            rows_processed = 0
            while True:
                count = await asyncio.to_thread(progress_queue.get)
                if count == -1:
                    return rows_processed
                rows_processed += count
                await self._update_job_progress(job_id, {
                    "rows_processed": rows_processed,
                    "current_table": table_key
                }, db_pool)
        
        listener_task = asyncio.create_task(progress_listener())
        
        try:
            with ProcessPoolExecutor(max_workers=len(worker_assignments)) as executor:
                futures = []
                
                for worker_id, (start_group, end_group) in enumerate(worker_assignments):
                    future = executor.submit(
                        _process_parquet_worker,
                        file_path, table_key, start_group, end_group,
                        commit_id, self.db_url, self.batch_size,
                        self.use_xxhash, self.xxhash_seed,
                        progress_queue, worker_id
                    )
                    futures.append(future)
                
                # Wait for completion
                for future in as_completed(futures):
                    future.result()  # Raises exception if worker failed
                
                # Signal completion
                progress_queue.put(-1)
                total_processed = await listener_task
                
        except Exception:
            listener_task.cancel()
            raise
        
        return total_processed
    
    async def _commit_batch(
        self,
        batch: List[Tuple[int, Dict]],
        table_key: str,
        commit_id: str,
        db_pool: DatabasePool
    ) -> None:
        """Commit a batch using efficient CTE query."""
        if not batch:
            return
        
        # Check parameter limit (PostgreSQL max is 32767)
        if len(batch) * 3 > 32000:  # Leave some buffer
            # Split into smaller batches if needed
            mid = len(batch) // 2
            await self._commit_batch(batch[:mid], table_key, commit_id, db_pool)
            await self._commit_batch(batch[mid:], table_key, commit_id, db_pool)
            return
        
        # Prepare parameters
        params = []
        for line_number, row_data in batch:
            # Convert datetime objects to ISO format strings
            row_data = self._convert_datetimes(row_data)
            data_json = json.dumps(row_data, sort_keys=True, separators=(',', ':'))
            data_hash = self._calculate_hash(data_json.encode('utf-8'))
            logical_row_id = f"{table_key}:{line_number}"
            params.extend([logical_row_id, data_hash, data_json])
        
        # Build VALUES clause
        values_template = ", ".join(
            f"(${i*3 + 1}, ${i*3 + 2}, ${i*3 + 3}::jsonb)"
            for i in range(len(batch))
        )
        
        # Single CTE query for atomic insertion
        query = f"""
            WITH new_data (logical_row_id, row_hash, data) AS (
                VALUES {values_template}
            ),
            inserted_rows AS (
                INSERT INTO dsa_core.rows (row_hash, data)
                SELECT row_hash, data FROM new_data
                ON CONFLICT (row_hash) DO NOTHING
            )
            INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
            SELECT ${len(params) + 1}, logical_row_id, row_hash FROM new_data
        """
        
        # Execute with all parameters
        async with db_pool.acquire() as conn:
            await conn.execute(query, *params, commit_id)
    
    async def _create_commit(
        self, db_pool: DatabasePool, dataset_id: int, 
        parent_commit_id: Optional[str], user_id: int, message: str
    ) -> str:
        """Create a new commit with proper Git-like SHA256 hash."""
        import hashlib
        from datetime import datetime
        
        # Create commit content for hashing (Git-like approach)
        timestamp = datetime.utcnow().isoformat()
        commit_content = {
            "dataset_id": dataset_id,
            "parent_commit_id": parent_commit_id or "",
            "author_id": user_id,
            "message": message,
            "timestamp": timestamp
        }
        
        # Generate SHA256 hash of commit content
        commit_json = json.dumps(commit_content, sort_keys=True, separators=(',', ':'))
        commit_id = hashlib.sha256(commit_json.encode('utf-8')).hexdigest()
        
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO dsa_core.commits "
                "(commit_id, dataset_id, parent_commit_id, author_id, message, authored_at, committed_at) "
                "VALUES ($1, $2, $3, $4, $5, NOW(), NOW())",
                commit_id, dataset_id, parent_commit_id, user_id, message
            )
        return commit_id
    
    async def _update_ref(
        self, db_pool: DatabasePool, dataset_id: int, ref_name: str, commit_id: str
    ) -> None:
        """Update reference to point to new commit."""
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO dsa_core.refs (dataset_id, name, commit_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (dataset_id, name) 
                DO UPDATE SET commit_id = $3
            """, dataset_id, ref_name, commit_id)
    
    async def _update_job_progress(
        self, job_id: str, progress_info: Dict[str, Any], db_pool: DatabasePool
    ) -> None:
        """Update job progress."""
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE dsa_jobs.analysis_runs "
                "SET run_parameters = jsonb_set(run_parameters, '{progress}', $1::jsonb, true) "
                "WHERE id = $2",
                json.dumps(progress_info), UUID(job_id)
            )
    
    async def _store_conversion_metadata(
        self, job_id: str, metadata: Dict[str, Any], db_pool: DatabasePool
    ) -> None:
        """Store conversion metadata in job parameters."""
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE dsa_jobs.analysis_runs "
                "SET run_parameters = jsonb_set(run_parameters, '{conversion_metadata}', $1::jsonb, true) "
                "WHERE id = $2",
                json.dumps(metadata), UUID(job_id)
            )
    
    async def _analyze_imported_tables(self, commit_id: str, db_pool: DatabasePool) -> None:
        """Analyze imported tables and store schema/statistics."""
        async with db_pool.acquire() as conn:
            # Get unique table keys
            table_keys = await conn.fetch("""
                SELECT DISTINCT split_part(logical_row_id, ':', 1) as table_key
                FROM dsa_core.commit_rows
                WHERE commit_id = $1
            """, commit_id)
            
            for record in table_keys:
                table_key = record['table_key']
                
                # Get sample rows
                sample_rows = await conn.fetch("""
                    SELECT r.data
                    FROM dsa_core.commit_rows cr
                    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                    WHERE cr.commit_id = $1 
                    AND cr.logical_row_id LIKE $2 || ':%'
                    LIMIT 1000
                """, commit_id, table_key)
                
                if not sample_rows:
                    continue
                
                # Analyze schema
                all_columns = set()
                column_types = {}
                null_counts = {}
                unique_values = {}
                
                for row in sample_rows:
                    row_data = row['data']
                    if not isinstance(row_data, dict):
                        continue
                    
                    for col, value in row_data.items():
                        all_columns.add(col)
                        
                        if col not in null_counts:
                            null_counts[col] = 0
                        if value is None:
                            null_counts[col] += 1
                        
                        if col not in unique_values:
                            unique_values[col] = set()
                        if value is not None and len(unique_values[col]) < 100:
                            unique_values[col].add(str(value))
                        
                        # Type inference
                        if value is not None and col not in column_types:
                            if isinstance(value, bool):
                                column_types[col] = 'boolean'
                            elif isinstance(value, int):
                                column_types[col] = 'integer'
                            elif isinstance(value, float):
                                column_types[col] = 'float'
                            else:
                                column_types[col] = 'string'
                
                # Get row count
                count_result = await conn.fetchrow("""
                    SELECT COUNT(*) as total
                    FROM dsa_core.commit_rows
                    WHERE commit_id = $1 AND logical_row_id LIKE $2 || ':%'
                """, commit_id, table_key)
                total_rows = count_result['total']
                
                # Build and store analysis
                analysis = {
                    'total_rows': total_rows,
                    'column_types': column_types,
                    'columns': sorted(list(all_columns)),
                    'null_counts': null_counts,
                    'unique_counts': {col: len(vals) for col, vals in unique_values.items()},
                    'sample_size': len(sample_rows)
                }
                
                await conn.execute("""
                    INSERT INTO dsa_core.table_analysis (commit_id, table_key, analysis)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (commit_id, table_key) 
                    DO UPDATE SET analysis = $3, created_at = NOW()
                """, commit_id, table_key, json.dumps(analysis))
                
                # Update commit_schemas using ON CONFLICT with JSONB merge
                schema_data = {
                    table_key: {
                        "columns": [
                            {"name": col, "type": column_types.get(col, "string")} 
                            for col in sorted(all_columns)
                        ]
                    }
                }
                
                # Use JSONB operators to merge schemas atomically
                await conn.execute("""
                    INSERT INTO dsa_core.commit_schemas (commit_id, schema_definition)
                    VALUES ($1, $2::jsonb)
                    ON CONFLICT (commit_id) DO UPDATE
                    SET schema_definition = dsa_core.commit_schemas.schema_definition || $2::jsonb
                """, commit_id, json.dumps(schema_data))
    
    async def _run_post_import_maintenance(self, commit_id: str, job_id: str, db_pool: DatabasePool) -> None:
        """Run post-import maintenance tasks."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Import job {job_id} - Analyzing imported tables")
        await self._analyze_imported_tables(commit_id, db_pool)
        
        logger.info(f"Import job {job_id} - Running VACUUM ANALYZE")
        async with db_pool.acquire() as conn:
            await conn.execute("SET statement_timeout = '30min';")
            await conn.execute("VACUUM (VERBOSE, ANALYZE) dsa_core.rows;")
            await conn.execute("VACUUM (VERBOSE, ANALYZE) dsa_core.commit_rows;")
        
        logger.info(f"Import job {job_id} - Refreshing search index")
        async with db_pool.acquire() as conn:
            await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_search.datasets_summary;")


# Worker function for parallel processing
def _process_parquet_worker(
    file_path: str, table_key: str, start_group: int, end_group: int,
    commit_id: str, db_url: str, batch_size: int,
    use_xxhash: bool, xxhash_seed: int,
    progress_queue: mp.Queue, worker_id: int
) -> int:
    """Process specific row groups from a Parquet file."""
    import polars as pl
    import json
    import xxhash
    import hashlib
    import psycopg
    import logging
    import pyarrow.parquet as pq
    from datetime import datetime, date
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(f"worker_{worker_id}")
    
    def calculate_hash(data: bytes) -> str:
        if use_xxhash:
            return xxhash.xxh64(data, seed=xxhash_seed).hexdigest()
        else:
            return hashlib.sha256(data).hexdigest()
    
    def convert_datetimes_worker(obj):
        """Convert datetime objects to ISO strings."""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: convert_datetimes_worker(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_datetimes_worker(item) for item in obj]
        else:
            return obj
    
    try:
        # Open Parquet file and read specific row groups
        parquet_file = pq.ParquetFile(file_path)
        total_rows = 0
        
        # Calculate starting line number based on previous row groups
        starting_line = 2  # Start at line 2
        for i in range(start_group):
            starting_line += parquet_file.metadata.row_group(i).num_rows
        
        with psycopg.connect(db_url) as conn:
            # Optimize connection
            with conn.cursor() as cur:
                cur.execute("SET work_mem = '256MB';")
                cur.execute("SET maintenance_work_mem = '256MB';")
                cur.execute("SET synchronous_commit = OFF;")
                cur.execute("""
                    CREATE TEMP TABLE import_batch (
                        logical_row_id TEXT, 
                        row_hash TEXT, 
                        data JSONB
                    ) ON COMMIT DROP
                """)
            
            current_line = starting_line
            
            # Process assigned row groups
            for rg_idx in range(start_group, end_group):
                row_group = parquet_file.read_row_group(rg_idx)
                df_batch = pl.from_arrow(row_group)
                
                batch_data = []
                for row in df_batch.iter_rows(named=True):
                    logical_row_id = f"{table_key}:{current_line}"
                    # Convert datetime objects to strings
                    row = convert_datetimes_worker(row)
                    data_json = json.dumps(row, sort_keys=True, separators=(',', ':'))
                    data_hash = calculate_hash(data_json.encode('utf-8'))
                    
                    batch_data.append((logical_row_id, data_hash, data_json))
                    current_line += 1
                    
                    if len(batch_data) >= batch_size:
                        _commit_batch_worker(conn, batch_data, commit_id)
                        progress_queue.put(len(batch_data))
                        total_rows += len(batch_data)
                        batch_data = []
                
                # Commit remaining batch
                if batch_data:
                    _commit_batch_worker(conn, batch_data, commit_id)
                    progress_queue.put(len(batch_data))
                    total_rows += len(batch_data)
        
        logger.info(f"Worker {worker_id} completed. Processed {total_rows} rows")
        return total_rows
        
    except Exception as e:
        logger.error(f"Worker {worker_id} failed: {e}", exc_info=True)
        raise


def _commit_batch_worker(conn, batch: List[Tuple[str, str, str]], commit_id: str):
    """Commit batch using efficient COPY."""
    if not batch:
        return
    
    buffer = io.StringIO()
    for logical_row_id, row_hash, data_json in batch:
        # Escape tabs and newlines in JSON to avoid COPY issues
        data_json = data_json.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
        buffer.write(f"{logical_row_id}\t{row_hash}\t{data_json}\n")
    
    buffer.seek(0)
    
    with conn.cursor() as cur:
        cur.execute("TRUNCATE import_batch;")
        
        with cur.copy("COPY import_batch (logical_row_id, row_hash, data) FROM STDIN") as copy:
            copy.write(buffer.read())
        
        with conn.transaction():
            cur.execute("""
                INSERT INTO dsa_core.rows (row_hash, data) 
                SELECT row_hash, data FROM import_batch 
                ON CONFLICT (row_hash) DO NOTHING
            """)
            cur.execute("""
                INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash) 
                SELECT %s, logical_row_id, row_hash FROM import_batch
            """, (commit_id,))