"""Optimized executor for import jobs with batch processing and bulk operations."""

import os
import csv
import json
import hashlib
import asyncio
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from uuid import UUID
import aiofiles
import tempfile

from src.workers.job_worker import JobExecutor
from src.infrastructure.postgres.database import DatabasePool
from src.infrastructure.config import get_settings
from src.infrastructure.services import FileParserFactory
from src.core.services.table_analyzer import TableAnalysisService


class ImportJobExecutor(JobExecutor):
    """Executes import jobs with optimized batch processing."""
    
    def __init__(self):
        self.parser_factory = FileParserFactory()
        self.settings = get_settings()
        self.batch_size = self.settings.import_batch_size
        self.chunk_size = self.settings.import_chunk_size
        self.table_analyzer = None  # Will be initialized with UoW in execute method
    
    async def execute(self, job_id: str, parameters: Dict[str, Any], db_pool: DatabasePool) -> Dict[str, Any]:
        """Execute import job with batch processing."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Import job {job_id} - Starting optimized import")
        
        # Handle case where parameters come as string
        if isinstance(parameters, str):
            parameters = json.loads(parameters)
        
        # Extract parameters
        temp_file_path = parameters['temp_file_path']
        filename = parameters['filename']
        commit_message = parameters['commit_message']
        target_ref = parameters['target_ref']
        file_size = parameters.get('file_size', 0)
        dataset_id = parameters['dataset_id']
        user_id = parameters['user_id']
        
        # Get job details from database
        async with db_pool.acquire() as conn:
            job = await conn.fetchrow(
                "SELECT source_commit_id FROM dsa_jobs.analysis_runs WHERE id = $1",
                UUID(job_id)
            )
            parent_commit_id = job['source_commit_id']
        
        try:
            # Update job progress - starting
            await self._update_job_progress(job_id, {"status": "Parsing file", "percentage": 0}, db_pool)
            
            # Determine file type and process with appropriate method
            file_ext = os.path.splitext(filename)[1].lower()
            
            if file_ext == '.csv':
                commit_id = await self._process_csv_file(
                    temp_file_path, filename, dataset_id, parent_commit_id,
                    user_id, commit_message, target_ref, job_id, file_size, db_pool
                )
            elif file_ext in ['.xlsx', '.xls']:
                commit_id = await self._process_excel_file(
                    temp_file_path, filename, dataset_id, parent_commit_id,
                    user_id, commit_message, target_ref, job_id, file_size, db_pool
                )
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
            
            # Refresh search index
            logger.info(f"Import job {job_id} - Refreshing search index")
            async with db_pool.acquire() as conn:
                await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_search.datasets_summary")
            
            # Clean up temp file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            
            # Get final statistics
            async with db_pool.acquire() as conn:
                stats = await conn.fetchrow("""
                    SELECT COUNT(*) as row_count 
                    FROM dsa_core.commit_rows 
                    WHERE commit_id = $1
                """, commit_id)
            
            return {
                "commit_id": commit_id,
                "rows_imported": stats['row_count'],
                "message": f"Successfully imported {stats['row_count']:,} rows"
            }
            
        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            raise
    
    async def _process_csv_file(
        self, file_path: str, filename: str, dataset_id: int,
        parent_commit_id: Optional[str], user_id: int, commit_message: str,
        target_ref: str, job_id: str, file_size: int, db_pool: DatabasePool
    ) -> str:
        """Process CSV file with batch processing."""
        sheet_name = os.path.splitext(filename)[0]
        all_row_mappings = []
        bytes_processed = 0
        
        # Create commit first
        commit_id = await self._create_commit(
            db_pool, dataset_id, parent_commit_id, user_id, commit_message
        )
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            batch = []
            row_count = 0
            
            for row_data in reader:
                batch.append(row_data)
                
                if len(batch) >= self.batch_size:
                    # Process batch
                    mappings = await self._process_batch(
                        batch, sheet_name, row_count, commit_id, db_pool
                    )
                    all_row_mappings.extend(mappings)
                    row_count += len(batch)
                    
                    # Update progress
                    bytes_processed = f.tell()
                    progress_pct = (bytes_processed / file_size * 100) if file_size > 0 else 0
                    await self._update_job_progress(
                        job_id,
                        {
                            "status": f"Processing: {row_count:,} rows",
                            "percentage": round(progress_pct, 2),
                            "rows_processed": row_count
                        },
                        db_pool
                    )
                    
                    batch = []
            
            # Process remaining rows
            if batch:
                mappings = await self._process_batch(
                    batch, sheet_name, row_count, commit_id, db_pool
                )
                all_row_mappings.extend(mappings)
        
        # Create commit_rows entries
        await self._create_commit_rows(commit_id, all_row_mappings, db_pool)
        
        # Update ref to point to new commit
        await self._update_ref(db_pool, dataset_id, target_ref, commit_id)
        
        # Analyze tables and store comprehensive analysis
        # Initialize table analyzer with UoW
        from src.infrastructure.postgres.unit_of_work import UnitOfWork
        async with db_pool.acquire() as conn:
            uow = UnitOfWork(conn)
            self.table_analyzer = TableAnalysisService(uow)
            await self.table_analyzer.analyze_committed_tables(commit_id)
        
        return commit_id
    
    async def _process_excel_file(
        self, file_path: str, filename: str, dataset_id: int,
        parent_commit_id: Optional[str], user_id: int, commit_message: str,
        target_ref: str, job_id: str, file_size: int, db_pool: DatabasePool
    ) -> str:
        """Process Excel file with batch processing using openpyxl for memory efficiency."""
        import openpyxl
        
        all_row_mappings = []
        
        # Create commit first
        commit_id = await self._create_commit(
            db_pool, dataset_id, parent_commit_id, user_id, commit_message
        )
        
        # Open workbook in read-only mode for memory efficiency
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        
        total_rows_processed = 0
        
        for sheet_name in wb.sheetnames:
            sheet = wb[sheet_name]
            batch = []
            headers = None
            row_count = 0
            
            for row_idx, row in enumerate(sheet.rows):
                if row_idx == 0:
                    # First row is headers
                    headers = [cell.value for cell in row]
                    continue
                
                # Convert row to dictionary
                row_data = {
                    headers[i]: cell.value
                    for i, cell in enumerate(row)
                    if i < len(headers)
                }
                
                batch.append(row_data)
                
                if len(batch) >= self.batch_size:
                    # Process batch
                    mappings = await self._process_batch(
                        batch, sheet_name, row_count, commit_id, db_pool
                    )
                    all_row_mappings.extend(mappings)
                    row_count += len(batch)
                    total_rows_processed += len(batch)
                    
                    # Update progress
                    await self._update_job_progress(
                        job_id,
                        {
                            "status": f"Processing {sheet_name}: {row_count:,} rows",
                            "rows_processed": total_rows_processed
                        },
                        db_pool
                    )
                    
                    batch = []
            
            # Process remaining rows for this sheet
            if batch:
                mappings = await self._process_batch(
                    batch, sheet_name, row_count, commit_id, db_pool
                )
                all_row_mappings.extend(mappings)
                total_rows_processed += len(batch)
        
        wb.close()
        
        # Create commit_rows entries
        await self._create_commit_rows(commit_id, all_row_mappings, db_pool)
        
        # Update ref to point to new commit
        await self._update_ref(db_pool, dataset_id, target_ref, commit_id)
        
        # Analyze tables and store comprehensive analysis
        # Initialize table analyzer with UoW
        from src.infrastructure.postgres.unit_of_work import UnitOfWork
        async with db_pool.acquire() as conn:
            uow = UnitOfWork(conn)
            self.table_analyzer = TableAnalysisService(uow)
            await self.table_analyzer.analyze_committed_tables(commit_id)
        
        return commit_id
    
    async def _process_batch(
        self, batch: List[Dict], sheet_name: str, start_row_idx: int,
        commit_id: str, db_pool: DatabasePool
    ) -> List[Tuple[str, str]]:
        """Process a batch of rows using COPY for maximum performance."""
        row_mappings = []
        
        # Create temporary table name
        temp_table = f"temp_import_{commit_id[:8]}_{start_row_idx}"
        
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Create temp table
                await conn.execute(f"""
                    CREATE TEMP TABLE {temp_table} (
                        logical_row_id TEXT,
                        row_hash VARCHAR(64),
                        data JSONB
                    )
                """)
                
                # Prepare data for COPY
                copy_data = []
                for idx, row_data in enumerate(batch):
                    # Create standardized row format
                    row = {
                        "sheet_name": sheet_name,
                        "row_number": start_row_idx + idx + 2,  # 1-indexed, +1 for header
                        "data": row_data
                    }
                    
                    # Calculate hash
                    row_json = json.dumps(row, sort_keys=True)
                    row_hash = hashlib.sha256(row_json.encode()).hexdigest()
                    
                    # Create logical row ID
                    logical_row_id = f"{sheet_name}:{start_row_idx + idx}"
                    
                    copy_data.append((logical_row_id, row_hash, row_json))
                    row_mappings.append((logical_row_id, row_hash))
                
                # Use COPY to bulk insert into temp table
                await conn.copy_records_to_table(
                    temp_table,
                    records=copy_data,
                    columns=['logical_row_id', 'row_hash', 'data']
                )
                
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
        
        return row_mappings
    
    async def _create_commit(
        self, db_pool: DatabasePool, dataset_id: int, parent_commit_id: Optional[str],
        user_id: int, message: str
    ) -> str:
        """Create a new commit."""
        import uuid
        commit_id = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO dsa_core.commits 
                    (commit_id, dataset_id, parent_commit_id, author_id, message,
                     authored_at, committed_at)
                VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
            """, commit_id, dataset_id, parent_commit_id, user_id, message)
        
        return commit_id
    
    async def _create_commit_rows(
        self, commit_id: str, row_mappings: List[Tuple[str, str]], db_pool: DatabasePool
    ) -> None:
        """Create commit_rows entries using bulk insert."""
        if not row_mappings:
            return
        
        # Create temporary table for bulk insert
        temp_table = f"temp_commit_rows_{commit_id[:8]}"
        
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Create temp table
                await conn.execute(f"""
                    CREATE TEMP TABLE {temp_table} (
                        commit_id CHAR(64),
                        logical_row_id TEXT,
                        row_hash CHAR(64)
                    )
                """)
                
                # Prepare data for COPY
                copy_data = [
                    (commit_id, logical_row_id, row_hash)
                    for logical_row_id, row_hash in row_mappings
                ]
                
                # Bulk insert using COPY
                await conn.copy_records_to_table(
                    temp_table,
                    records=copy_data,
                    columns=['commit_id', 'logical_row_id', 'row_hash']
                )
                
                # Insert into commit_rows
                await conn.execute(f"""
                    INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
                    SELECT * FROM {temp_table}
                """)
                
                # Drop temp table
                await conn.execute(f"DROP TABLE {temp_table}")
    
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