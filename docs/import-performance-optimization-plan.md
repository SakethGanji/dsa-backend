# Import Endpoint Performance Optimization Plan

## Executive Summary

The current `/import` endpoint implementation has critical performance bottlenecks that prevent efficient handling of gigabyte-scale files. This document outlines the issues and provides a comprehensive optimization plan.

## Schema Alignment Notes

**IMPORTANT**: This plan has been updated to align with the actual DSA schema:

1. **Jobs Table**: Uses `dsa_jobs.analysis_runs` instead of `dsa_core.jobs`
   - `id` is a UUID (not serial)
   - `status` uses `dsa_jobs.analysis_run_status` enum
   - `run_type` uses `dsa_jobs.analysis_run_type` enum  
   - Progress stored in `run_parameters` JSONB (until schema updated)

2. **Git-like Versioning**: Imports must create commits
   - Each import creates a new commit with parent reference
   - Rows are linked via `commit_rows` table with logical IDs
   - Main branch ref is updated to point to new commit

3. **Search Indexing**: Uses materialized views
   - `REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_search.datasets_summary`
   - Run after successful import completion

## Critical Issues Identified

### 1. Full File Loading in Memory
**Location:** `queue_import_job.py:52`
- **Issue:** Entire file is read into memory with `file.read()`
- **Impact:** Memory exhaustion for GB-scale files
- **Current code:**
  ```python
  content = await file.read()  # Loads entire file into memory
  ```

### 2. In-Memory Row Processing
**Location:** `import_executor.py:98-155`
- **Issue:** All rows are parsed and stored in a list before database insertion
- **Impact:** Memory grows linearly with file size
- **Problems:**
  - CSV: Entire file loaded at once
  - Excel: pandas loads entire file into memory
  - No streaming or batch processing

### 3. Inefficient Database Operations
**Location:** `import_executor.py:173-212`
- **Issue:** Individual INSERT queries for each row
- **Impact:** 
  - Network latency multiplied by row count
  - Transaction overhead for potentially millions of rows
  - No bulk insert optimization

### 4. Additional Performance Issues
- **Missing database indexes** on `rows.row_hash`
- **Full search index refresh** after every import
- **No connection pooling optimization**
- **Entire import in single transaction**
- **No file size validation enforcement**

## Optimization Recommendations

### 1. Implement Streaming File Upload

Replace current file reading with streaming chunks:

```python
# queue_import_job.py
chunk_size = 1024 * 1024  # 1MB chunks
temp_path = f"/tmp/{file.filename}"

async with aiofiles.open(temp_path, 'wb') as f:
    while chunk := await file.read(chunk_size):
        await f.write(chunk)
        # Optional: Update progress
```

### 2. Implement Batch Processing for CSV

Process CSV files in batches to limit memory usage:

```python
# import_executor.py
import csv

BATCH_SIZE = 10000

async def process_csv_file(file_path, conn):
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        batch = []
        
        for row in reader:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                await process_batch(batch, conn)
                batch = []
        
        # Process remaining rows
        if batch:
            await process_batch(batch, conn)
```

### 3. Implement Bulk Database Operations with Git-like Commits

Use PostgreSQL's COPY command for bulk inserts and create proper commits:

```python
# import_executor.py
import hashlib
import json
from datetime import datetime
from typing import List, Dict, Tuple

async def process_batch(batch: List[Dict], sheet_name: str, start_row_idx: int, 
                       conn, update_existing=False) -> List[Tuple[str, str]]:
    """Process a batch of rows using COPY for maximum performance.
    
    Args:
        batch: List of row dictionaries to insert
        sheet_name: Name of the sheet/file being processed
        start_row_idx: Starting row index for this batch
        conn: Database connection
        update_existing: If True, update rows with same hash but different data
        
    Returns:
        List of tuples (logical_row_id, row_hash) for commit_rows creation
    """
    # Create temporary table
    temp_table = f"temp_import_{uuid.uuid4().hex}"
    
    # Use transaction for batch atomicity
    async with conn.transaction():
        await conn.execute(f"""
            CREATE TEMP TABLE {temp_table} (
                logical_row_id TEXT,
                row_hash VARCHAR(64),
                data JSONB
            )
        """)
        
        # Use COPY for bulk insert into temp table
        row_mappings = []
        async with conn.copy_writer(f'COPY {temp_table} (logical_row_id, row_hash, data) FROM STDIN') as copy:
            for idx, row in enumerate(batch):
                # Create standardized row format
                row_data = {
                    "sheet_name": sheet_name,
                    "row_number": start_row_idx + idx + 1,  # 1-indexed
                    "data": row
                }
                row_hash = hashlib.sha256(json.dumps(row_data, sort_keys=True).encode()).hexdigest()
                logical_row_id = f"{sheet_name}:{start_row_idx + idx + 1}"
                
                await copy.write_row([logical_row_id, row_hash, json.dumps(row_data)])
                row_mappings.append((logical_row_id, row_hash))
        
        # Insert only new rows into dsa_core.rows
        await conn.execute(f"""
            INSERT INTO dsa_core.rows (row_hash, data)
            SELECT DISTINCT t.row_hash, t.data
            FROM {temp_table} t
            LEFT JOIN dsa_core.rows r ON t.row_hash = r.row_hash
            WHERE r.row_hash IS NULL
        """)
        
        # Drop temp table
        await conn.execute(f"DROP TABLE {temp_table}")
        
        # Return mappings for commit_rows creation
        return row_mappings

async def create_import_commit(dataset_id: int, parent_commit_id: str, 
                              row_mappings: List[Tuple[str, str]], 
                              user_id: int, conn) -> str:
    """Create a new commit for the imported data.
    
    Args:
        dataset_id: ID of the dataset
        parent_commit_id: Parent commit (usually from main branch)
        row_mappings: List of (logical_row_id, row_hash) tuples
        user_id: ID of the user performing the import
        conn: Database connection
        
    Returns:
        New commit_id
    """
    # Generate commit ID (timestamp + uuid suffix for uniqueness)
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    commit_id = f"{timestamp}_{uuid.uuid4().hex[:8]}"
    
    async with conn.transaction():
        # Create the commit
        await conn.execute("""
            INSERT INTO dsa_core.commits 
                (commit_id, dataset_id, parent_commit_id, message, author_id, 
                 authored_at, committed_at)
            VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
        """, commit_id, dataset_id, parent_commit_id, 
            f"Import {len(row_mappings)} rows", user_id)
        
        # Create commit_rows entries using temporary table for performance
        temp_table = f"temp_commit_rows_{uuid.uuid4().hex}"
        await conn.execute(f"""
            CREATE TEMP TABLE {temp_table} (
                commit_id CHAR(64),
                logical_row_id TEXT,
                row_hash CHAR(64)
            )
        """)
        
        # Bulk insert commit_rows
        async with conn.copy_writer(f'COPY {temp_table} FROM STDIN') as copy:
            for logical_row_id, row_hash in row_mappings:
                await copy.write_row([commit_id, logical_row_id, row_hash])
        
        await conn.execute(f"""
            INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
            SELECT * FROM {temp_table}
        """)
        
        # Update the main branch to point to new commit
        await conn.execute("""
            UPDATE dsa_core.refs 
            SET commit_id = $1
            WHERE dataset_id = $2 AND name = 'main'
        """, commit_id, dataset_id)
        
        await conn.execute(f"DROP TABLE {temp_table}")
    
    return commit_id
```

### 4. Add Database Indexes

Create indexes to improve lookup performance:

```sql
-- Add to migration or setup script
CREATE INDEX idx_rows_row_hash ON dsa_core.rows(row_hash);
CREATE INDEX idx_rows_created_at ON dsa_core.rows(created_at);
CREATE INDEX idx_commit_rows_commit_hash ON dsa_core.commit_rows(commit_hash);
```

### 5. Optimize Excel Processing

Use memory-efficient Excel reading:

```python
# import_executor.py
import openpyxl

async def process_excel_file(file_path, conn):
    wb = openpyxl.load_workbook(file_path, read_only=True)
    
    for sheet in wb.worksheets:
        batch = []
        headers = None
        
        for row_idx, row in enumerate(sheet.rows):
            if row_idx == 0:
                headers = [cell.value for cell in row]
                continue
            
            row_data = {headers[i]: cell.value for i, cell in enumerate(row)}
            batch.append(row_data)
            
            if len(batch) >= BATCH_SIZE:
                await process_batch(batch, conn)
                batch = []
        
        if batch:
            await process_batch(batch, conn)
```

### 6. Implement Progress Tracking

Add progress updates for long-running imports:

```python
# import_executor.py
import os

async def update_job_progress(job_id: UUID, progress_info, conn):
    await conn.execute("""
        UPDATE dsa_jobs.analysis_runs 
        SET run_parameters = run_parameters || jsonb_build_object('progress', $1::jsonb)
        WHERE id = $2
    """, json.dumps(progress_info), job_id)

# Option 1: Progress by rows processed (no total needed)
async def track_progress_by_rows(job_id, conn):
    rows_processed = 0
    for batch_num, batch in enumerate(process_file_in_batches(file_path)):
        await process_batch(batch, conn)
        rows_processed += len(batch)
        
        # Update every 10 batches
        if batch_num % 10 == 0:
            progress_info = {
                "rows_processed": rows_processed,
                "status": f"Processed {rows_processed:,} rows"
            }
            await update_job_progress(job_id, progress_info, conn)

# Option 2: Progress by bytes processed (for streaming uploads)
async def track_progress_by_bytes(job_id, file_path, conn):
    file_size = os.path.getsize(file_path)
    bytes_processed = 0
    
    async with aiofiles.open(file_path, 'rb') as f:
        while chunk := await f.read(CHUNK_SIZE):
            bytes_processed += len(chunk)
            progress_pct = (bytes_processed / file_size) * 100
            
            progress_info = {
                "bytes_processed": bytes_processed,
                "total_bytes": file_size,
                "percentage": round(progress_pct, 2),
                "status": f"Processing: {progress_pct:.1f}%"
            }
            await update_job_progress(job_id, progress_info, conn)
```

### 7. Implement File Size Validation with Cleanup

Enforce the configured file size limit with proper cleanup:

```python
# queue_import_job.py
import tempfile
import contextlib

MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5GB from config

@contextlib.asynccontextmanager
async def save_upload_file_tmp(upload_file: UploadFile, max_size: int):
    """Save uploaded file to temp location with size validation and cleanup."""
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
        file_size = 0
        chunk_size = 1024 * 1024  # 1MB
        
        async with aiofiles.open(temp_file.name, 'wb') as f:
            while chunk := await upload_file.read(chunk_size):
                file_size += len(chunk)
                if file_size > max_size:
                    raise HTTPException(
                        status_code=413,
                        detail=f"File size ({file_size:,} bytes) exceeds maximum allowed size ({max_size:,} bytes)"
                    )
                await f.write(chunk)
        
        yield temp_file.name
    finally:
        # Always cleanup temp file
        try:
            os.unlink(temp_file.name)
        except OSError:
            pass  # File already deleted

# Usage in endpoint
@router.post("/import")
async def import_file(file: UploadFile = File(...)):
    async with save_upload_file_tmp(file, MAX_FILE_SIZE) as temp_path:
        # Process the file
        await queue_import_job(temp_path, file.filename)
```

### 8. Search Index Updates

The DSA platform uses materialized views for search performance. After a successful import:

```python
# import_executor.py
async def refresh_search_indexes(conn):
    """Refresh the search materialized view after data changes.
    
    Note: This uses CONCURRENTLY to avoid blocking reads, but it:
    - Requires more resources (builds a new view alongside the old one)
    - Takes longer than a regular refresh
    - Cannot be run inside a transaction
    """
    # This must be run outside of any transaction
    await conn.execute("""
        REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_search.datasets_summary
    """)

# Alternative: For very large imports, consider deferring the refresh
async def schedule_search_refresh(dataset_id: int, conn):
    """Schedule a search index refresh as a separate job."""
    await conn.execute("""
        INSERT INTO dsa_jobs.analysis_runs 
            (id, run_type, status, dataset_id, run_parameters, user_id, created_at)
        VALUES 
            (gen_random_uuid(), 'maintenance'::dsa_jobs.analysis_run_type, 
             'pending'::dsa_jobs.analysis_run_status, $1, 
             '{"task": "refresh_search_index"}'::jsonb, 1, NOW())
    """, dataset_id)

# Integration with batch processing
async def process_csv_file(file_path: str, dataset_id: int, parent_commit_id: str, 
                          user_id: int, conn) -> str:
    """Process CSV file and create a commit with all imported data."""
    sheet_name = os.path.basename(file_path).split('.')[0]
    all_row_mappings = []
    
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        batch = []
        row_count = 0
        
        for row in reader:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                mappings = await process_batch(batch, sheet_name, row_count, conn)
                all_row_mappings.extend(mappings)
                row_count += len(batch)
                batch = []
        
        # Process remaining rows
        if batch:
            mappings = await process_batch(batch, sheet_name, row_count, conn)
            all_row_mappings.extend(mappings)
    
    # Create commit with all imported rows
    commit_id = await create_import_commit(
        dataset_id, parent_commit_id, all_row_mappings, user_id, conn
    )
    
    # Refresh search index
    await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_search.datasets_summary")
    
    return commit_id
```

### 9. Parallel Import Job Processing

Enable concurrent processing of multiple import jobs:

```python
# import_executor.py
import asyncio
from typing import List

class ImportJobProcessor:
    """Manages parallel processing of multiple import jobs."""
    
    def __init__(self, pool, max_concurrent_jobs=4):
        self.pool = pool
        self.max_concurrent_jobs = max_concurrent_jobs
        self.semaphore = asyncio.Semaphore(max_concurrent_jobs)
    
    async def process_job(self, job_id: UUID, file_path: str, run_parameters: dict):
        """Process a single import job with connection management.
        
        Args:
            job_id: UUID of the analysis_run
            file_path: Path to the file to import
            run_parameters: JSONB parameters including dataset_id and parent_commit_id
        """
        async with self.semaphore:  # Limit concurrent jobs
            conn = await self.pool.acquire()
            try:
                # Extract parameters
                dataset_id = run_parameters['dataset_id']
                parent_commit_id = run_parameters.get('parent_commit_id')
                user_id = run_parameters['user_id']
                
                # If no parent commit specified, get from main branch
                if not parent_commit_id:
                    result = await conn.fetchrow("""
                        SELECT commit_id FROM dsa_core.refs 
                        WHERE dataset_id = $1 AND name = 'main'
                    """, dataset_id)
                    parent_commit_id = result['commit_id'] if result else None
                
                # Determine file type and process
                if file_path.endswith('.csv'):
                    commit_id = await process_csv_file(
                        file_path, dataset_id, parent_commit_id, user_id, conn
                    )
                elif file_path.endswith(('.xlsx', '.xls')):
                    commit_id = await process_excel_file(
                        file_path, dataset_id, parent_commit_id, user_id, conn
                    )
                else:
                    raise ValueError(f"Unsupported file type: {file_path}")
                
                # Mark job as completed with output
                output_summary = {
                    "commit_id": commit_id,
                    "status": "Import completed successfully"
                }
                await conn.execute("""
                    UPDATE dsa_jobs.analysis_runs 
                    SET status = 'completed'::dsa_jobs.analysis_run_status,
                        output_summary = $1::jsonb,
                        completed_at = NOW()
                    WHERE id = $2
                """, json.dumps(output_summary), job_id)
                
            except Exception as e:
                # Mark job as failed
                await conn.execute("""
                    UPDATE dsa_jobs.analysis_runs 
                    SET status = 'failed'::dsa_jobs.analysis_run_status,
                        error_message = $1,
                        completed_at = NOW()
                    WHERE id = $2
                """, str(e), job_id)
                raise
            finally:
                await self.pool.release(conn)
    
    async def process_pending_jobs(self):
        """Process all pending import jobs in parallel."""
        conn = await self.pool.acquire()
        try:
            # Get pending import jobs
            pending_jobs = await conn.fetch("""
                SELECT id, run_parameters 
                FROM dsa_jobs.analysis_runs 
                WHERE status = 'pending'::dsa_jobs.analysis_run_status 
                  AND run_type = 'import'::dsa_jobs.analysis_run_type
                ORDER BY created_at
            """)
            
            # Process jobs in parallel
            tasks = [
                self.process_job(
                    job['id'], 
                    job['run_parameters']['file_path'],
                    job['run_parameters']
                )
                for job in pending_jobs
            ]
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
        finally:
            await self.pool.release(conn)

# Note: Intra-file parallelism (splitting a single file) is NOT recommended
# due to complexity with CSV/Excel format boundaries. Focus on job-level parallelism.
```

### 10. Add Progress and Checkpoint Support to Schema

First, add the missing columns to the analysis_runs table:

```sql
-- Add progress and checkpoint columns to analysis_runs table
ALTER TABLE dsa_jobs.analysis_runs 
    ADD COLUMN IF NOT EXISTS progress JSONB,
    ADD COLUMN IF NOT EXISTS checkpoint JSONB;

-- Create index for finding resumable jobs
CREATE INDEX IF NOT EXISTS idx_analysis_runs_checkpoint 
    ON dsa_jobs.analysis_runs(id) 
    WHERE checkpoint IS NOT NULL;
```

Then implement checkpointing:

```python
# import_executor.py
async def save_checkpoint(job_id: UUID, checkpoint_data: dict, conn):
    """Save checkpoint data for resumable imports."""
    await conn.execute("""
        UPDATE dsa_jobs.analysis_runs
        SET checkpoint = $1::jsonb
        WHERE id = $2
    """, json.dumps(checkpoint_data), job_id)

async def resume_from_checkpoint(job_id: UUID, conn):
    """Resume from a previous checkpoint if available."""
    result = await conn.fetchrow("""
        SELECT checkpoint FROM dsa_jobs.analysis_runs WHERE id = $1
    """, job_id)
    
    if result and result['checkpoint']:
        return result['checkpoint']  # Already JSONB
    return None
```

## Implementation Priority

1. **High Priority (Immediate)**
   - Streaming file upload (prevents memory exhaustion)
   - Batch processing for CSV/Excel
   - Add database indexes
   - File size validation with cleanup
   - Bulk database operations with COPY
   - Progress tracking (bytes-based approach)
   - Transaction scope optimization

## Performance Expectations

With these optimizations:
- **Memory usage**: Constant regardless of file size (streaming + batching)
- **Import speed**: 10-100x faster for large files (bulk operations)
- **Scalability**: Handle files up to configured 5GB limit
- **Reliability**: Progress tracking and resumable imports

## Testing Recommendations

1. Create test files of various sizes (1MB, 100MB, 1GB, 5GB)
2. Monitor memory usage during import
3. Benchmark import times before and after optimization
4. Test error handling and recovery
5. Verify data integrity after bulk operations

## Configuration Updates

Add these settings to `config.py`:

```python
# Import settings
IMPORT_BATCH_SIZE = int(os.getenv("IMPORT_BATCH_SIZE", "10000"))
IMPORT_CHUNK_SIZE = int(os.getenv("IMPORT_CHUNK_SIZE", "1048576"))  # 1MB
IMPORT_PARALLEL_WORKERS = int(os.getenv("IMPORT_PARALLEL_WORKERS", "4"))
IMPORT_PROGRESS_UPDATE_INTERVAL = int(os.getenv("IMPORT_PROGRESS_UPDATE_INTERVAL", "10"))
```

## Monitoring

Add metrics for:
- Import duration by file size
- Memory usage during import
- Rows processed per second
- Database connection pool usage
- Error rates by file type

## Conclusion

The current implementation is not suitable for production use with large files. These optimizations will enable the system to handle gigabyte-scale imports efficiently while maintaining data integrity and providing a good user experience.

## Key Design Decisions

1. **Progress Tracking**: Use bytes-based progress for accurate percentages without the overhead of counting rows
2. **Transaction Boundaries**: Each batch runs in its own transaction, preventing long-running locks
3. **Parallelism**: Focus on job-level parallelism (multiple files) rather than intra-file parallelism
4. **Update Strategy**: Configurable behavior for handling duplicate rows (ignore vs. update)
5. **Cleanup**: Automatic cleanup of temporary files with proper error handling

## Additional Considerations

### Temporary File Management
- Implement a cleanup cron job for orphaned temp files:
  ```bash
  # Clean files older than 24 hours
  find /tmp -name "tmp*" -mtime +1 -delete
  ```

### Monitoring Alerts
- Alert on import jobs running longer than expected
- Monitor temp directory disk usage
- Track failed import rate

### Security
- Validate file types before processing
- Scan for malicious content if needed
- Implement rate limiting on import endpoints