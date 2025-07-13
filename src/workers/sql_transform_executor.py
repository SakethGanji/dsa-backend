"""Executor for SQL transformation jobs."""

import json
import hashlib
import time
from typing import Dict, Any, List, Tuple
from datetime import datetime
from uuid import uuid4
import asyncpg

from .job_worker import JobExecutor
from ..infrastructure.postgres.database import DatabasePool


class SqlTransformExecutor(JobExecutor):
    """Executes SQL transformation jobs."""
    
    async def execute(self, job_id: str, parameters: Dict[str, Any], db_pool: DatabasePool) -> Dict[str, Any]:
        """Execute SQL transformation job."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"SQL transform job {job_id} starting with parameters: {parameters}")
        
        # Handle case where parameters come as string
        if isinstance(parameters, str):
            parameters = json.loads(parameters)
        
        # Extract parameters
        sources = parameters['sources']
        sql = parameters['sql']
        target = parameters['target']
        
        # Get job details from database
        async with db_pool.acquire() as conn:
            job = await conn.fetchrow(
                "SELECT dataset_id, user_id, source_commit_id FROM dsa_jobs.analysis_runs WHERE id = $1",
                job_id
            )
            
            dataset_id = job['dataset_id']
            user_id = job['user_id']
            parent_commit_id = job['source_commit_id']
        
        try:
            start_time = time.time()
            rows_processed = 0
            
            async with db_pool.acquire() as conn:
                # Start transaction
                async with conn.transaction():
                    # Create temporary views for each source
                    view_names = await self._create_source_views(conn, sources, job_id)
                    
                    # Modify SQL to use temporary views
                    modified_sql = self._replace_aliases_with_views(sql, view_names)
                    
                    # Execute the transformation query
                    logger.info(f"Executing SQL transformation: {modified_sql[:200]}...")
                    result_rows = await conn.fetch(modified_sql)
                    
                    # Process results and create new commit
                    new_commit_id = await self._create_commit_with_results(
                        conn,
                        result_rows,
                        dataset_id,
                        parent_commit_id,
                        target['message'],
                        user_id,
                        target['table_key']
                    )
                    
                    rows_processed = len(result_rows)
                    
                    # Update ref to point to new commit
                    await conn.execute(
                        """
                        UPDATE dsa_core.refs 
                        SET commit_id = $1
                        WHERE dataset_id = $2 AND name = $3
                        """,
                        new_commit_id,
                        dataset_id,
                        target['ref']
                    )
                    
                    # Clean up temporary views
                    for _, view_name in view_names:
                        await conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            
            execution_time_ms = int((time.time() - start_time) * 1000)
            
            return {
                "rows_processed": rows_processed,
                "new_commit_id": new_commit_id,
                "execution_time_ms": execution_time_ms,
                "target_ref": target['ref'],
                "table_key": target['table_key']
            }
            
        except Exception as e:
            logger.error(f"SQL transform job {job_id} failed: {str(e)}", exc_info=True)
            raise
    
    async def _create_source_views(
        self,
        conn: asyncpg.Connection,
        sources: List[Dict[str, Any]],
        job_id: str
    ) -> List[Tuple[str, str]]:
        """Create temporary views for each source table."""
        view_names = []
        
        for source in sources:
            # Get commit ID for the source ref
            commit_row = await conn.fetchrow(
                """
                SELECT c.commit_id 
                FROM dsa_core.refs r
                JOIN dsa_core.commits c ON r.commit_id = c.commit_id
                WHERE r.dataset_id = $1 AND r.name = $2
                """,
                source['dataset_id'],
                source['ref']
            )
            
            if not commit_row:
                raise ValueError(f"Ref '{source['ref']}' not found for dataset {source['dataset_id']}")
            
            commit_id = commit_row['commit_id']
            
            # Create unique view name
            view_name = f"sql_transform_{source['alias']}_{job_id.replace('-', '_')}"
            
            # Create view with data from the commit
            # Extract and parse the nested data field - it's stored as a JSON string
            await conn.execute(f"""
                CREATE TEMPORARY VIEW {view_name} AS
                SELECT 
                    cr.logical_row_id,
                    CASE 
                        WHEN jsonb_typeof(r.data->'data') = 'string' THEN (r.data->>'data')::jsonb
                        ELSE r.data->'data'
                    END AS data
                FROM dsa_core.commit_rows cr
                JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = '{commit_id}'
            """)
            
            view_names.append((source['alias'], view_name))
        
        return view_names
    
    def _replace_aliases_with_views(self, sql: str, view_names: List[Tuple[str, str]]) -> str:
        """Replace table aliases with temporary view names in SQL."""
        modified_sql = sql
        
        # Sort by length descending to avoid partial replacements
        sorted_views = sorted(view_names, key=lambda x: len(x[0]), reverse=True)
        
        for alias, view_name in sorted_views:
            # Replace common patterns
            patterns = [
                (f" {alias} ", f" {view_name} "),  # Standalone alias
                (f" {alias}.", f" {view_name}.data."),   # Alias with dot - access JSONB data
                (f"FROM {alias}", f"FROM {view_name}"),  # FROM clause
                (f"JOIN {alias}", f"JOIN {view_name}"),  # JOIN clause
                (f"({alias})", f"({view_name})"),  # In parentheses
            ]
            
            for pattern, replacement in patterns:
                modified_sql = modified_sql.replace(pattern, replacement)
            
            # Handle SELECT * case for JSONB data
            if f"SELECT * FROM {view_name}" in modified_sql or f"SELECT *\nFROM {view_name}" in modified_sql:
                modified_sql = modified_sql.replace("SELECT *", "SELECT data")
        
        return modified_sql
    
    async def _create_commit_with_results(
        self,
        conn: asyncpg.Connection,
        result_rows: List[asyncpg.Record],
        dataset_id: int,
        parent_commit_id: str,
        message: str,
        user_id: int,
        table_key: str
    ) -> str:
        """Create a new commit with the transformation results."""
        # Generate new commit ID
        commit_id = hashlib.sha256(
            f"{parent_commit_id}:{message}:{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()
        
        # Create commit record
        await conn.execute(
            """
            INSERT INTO dsa_core.commits (
                commit_id, dataset_id, parent_commit_id, 
                message, author_id, committed_at
            ) VALUES ($1, $2, $3, $4, $5, $6)
            """,
            commit_id, dataset_id, parent_commit_id,
            message, user_id, datetime.utcnow()
        )
        
        # Process each result row
        for idx, row in enumerate(result_rows):
            # Handle different data structures
            row_dict = dict(row)
            
            # If we have a 'data' column that's already a dict/JSONB, use it directly
            if 'data' in row_dict and isinstance(row_dict['data'], dict):
                inner_data = row_dict['data']
            else:
                # Otherwise use the whole row
                inner_data = row_dict
            
            # Wrap in the expected structure with nested data field
            # Store as JSON string in the 'data' field to match existing format
            row_data = {
                "data": json.dumps(inner_data, default=str),
                "row_number": idx + 1,
                "sheet_name": table_key
            }
            row_json = json.dumps(row_data, default=str)
            
            # Calculate row hash
            row_hash = hashlib.sha256(row_json.encode()).hexdigest()
            
            # Insert row if not exists
            await conn.execute(
                """
                INSERT INTO dsa_core.rows (row_hash, data)
                VALUES ($1, $2::jsonb)
                ON CONFLICT (row_hash) DO NOTHING
                """,
                row_hash, row_json
            )
            
            # Link row to commit
            logical_row_id = row_dict.get('logical_row_id', row_dict.get('id', str(uuid4())))
            await conn.execute(
                """
                INSERT INTO dsa_core.commit_rows (
                    commit_id, logical_row_id, row_hash
                ) VALUES ($1, $2, $3)
                """,
                commit_id, str(logical_row_id), row_hash
            )
        
        return commit_id