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
            output_branch_name = None
            
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
                    
                    # Create branch for the output commit
                    # Use provided branch name or default to commit ID
                    output_branch_name = target.get('output_branch_name') or new_commit_id
                    await self._create_output_branch(
                        conn,
                        dataset_id,
                        output_branch_name,
                        new_commit_id
                    )
                    logger.info(f"Created output branch: {output_branch_name}")
                    
                    # Clean up temporary views
                    for _, view_name in view_names:
                        await conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            
            execution_time_ms = int((time.time() - start_time) * 1000)
            
            return {
                "rows_processed": rows_processed,
                "new_commit_id": new_commit_id,
                "output_branch_name": output_branch_name,
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
            
            # Create view with data from the commit filtered by table_key
            # Extract and parse the nested data field
            # Escape single quotes in table_key for SQL safety
            escaped_table_key = source['table_key'].replace("'", "''")
            
            await conn.execute(f"""
                CREATE TEMPORARY VIEW {view_name} AS
                SELECT 
                    cr.logical_row_id,
                    r.data->'data' as data
                FROM dsa_core.commit_rows cr
                JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = '{commit_id}'
                AND (cr.logical_row_id LIKE '{escaped_table_key}:%' 
                     OR cr.logical_row_id LIKE '{escaped_table_key}\\_%')
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
                (f" {alias}.", f" {view_name}."),   # Alias with dot - no need for nested access
                (f"FROM {alias}", f"FROM {view_name}"),  # FROM clause
                (f"JOIN {alias}", f"JOIN {view_name}"),  # JOIN clause
                (f"({alias})", f"({view_name})"),  # In parentheses
            ]
            
            for pattern, replacement in patterns:
                modified_sql = modified_sql.replace(pattern, replacement)
            
            # Handle SELECT * case - data is already the inner data field
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
        
        # First, copy all existing tables from parent commit EXCEPT the one we're replacing
        await conn.execute("""
            INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
            SELECT $1, logical_row_id, row_hash
            FROM dsa_core.commit_rows
            WHERE commit_id = $2
            AND NOT (logical_row_id LIKE $3 || ':%' OR logical_row_id LIKE $3 || '\\_%')
        """, commit_id, parent_commit_id, table_key)
        
        # Process each result row
        for idx, row in enumerate(result_rows):
            # Handle different data structures
            row_dict = dict(row)
            
            # The SQL query returns the actual column data
            # We need to wrap it in the standardized format
            # Check if the result has a 'data' column from SELECT * queries
            if 'data' in row_dict and len(row_dict) == 1:
                # This is from a SELECT * query that returned data->'data' as data
                inner_data = row_dict['data']
                # If it's a string, parse it as JSON
                if isinstance(inner_data, str):
                    inner_data = json.loads(inner_data)
            else:
                # This is from a query with specific columns
                inner_data = row_dict
            
            # Wrap in the expected structure with nested data field
            # Store data as object, not JSON string, to match standardized format
            row_data = {
                "sheet_name": table_key,
                "row_number": idx + 1,
                "data": inner_data  # Store as object, not JSON string
            }
            row_json = json.dumps(row_data, default=str)
            
            # Calculate row hash from data content only (not the wrapper)
            # This matches the hash calculation in create_commit.py and import_executor.py
            data_json = json.dumps(inner_data, sort_keys=True, separators=(',', ':'), default=str)
            row_hash = hashlib.sha256(data_json.encode()).hexdigest()
            
            # Insert row if not exists
            await conn.execute(
                """
                INSERT INTO dsa_core.rows (row_hash, data)
                VALUES ($1, $2::jsonb)
                ON CONFLICT (row_hash) DO NOTHING
                """,
                row_hash, row_json
            )
            
            # Link row to commit with proper logical_row_id format
            # If the row already has a logical_row_id (from source data), preserve it
            # Otherwise, create new one with table_key prefix
            if 'logical_row_id' in row_dict and row_dict['logical_row_id']:
                logical_row_id = row_dict['logical_row_id']
            else:
                # Create new logical_row_id with table_key:hash format
                # Use the same hash we calculated for the row_hash (data content only)
                logical_row_id = f"{table_key}:{row_hash}"
            
            await conn.execute(
                """
                INSERT INTO dsa_core.commit_rows (
                    commit_id, logical_row_id, row_hash
                ) VALUES ($1, $2, $3)
                """,
                commit_id, logical_row_id, row_hash
            )
        
        # Get schema from parent commit and update it
        parent_schema_row = await conn.fetchrow("""
            SELECT schema_definition
            FROM dsa_core.commit_schemas
            WHERE commit_id = $1
        """, parent_commit_id)
        
        # Parse existing schema or create new one
        if parent_schema_row and parent_schema_row['schema_definition']:
            parent_schema = json.loads(parent_schema_row['schema_definition'])
        else:
            parent_schema = {}
        
        # Infer schema for the new table from result rows
        if result_rows:
            # Get column names and types from first row
            first_row = dict(result_rows[0])
            if 'data' in first_row and isinstance(first_row['data'], dict):
                sample_data = first_row['data']
            else:
                sample_data = first_row
            
            # Build column schema
            columns = []
            for col_name, value in sample_data.items():
                col_type = 'text'  # Default
                if isinstance(value, bool):
                    col_type = 'boolean'
                elif isinstance(value, int):
                    col_type = 'integer'
                elif isinstance(value, float):
                    col_type = 'float'
                elif isinstance(value, str):
                    try:
                        float(value)
                        col_type = 'numeric'
                    except ValueError:
                        col_type = 'text'
                
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': True
                })
            
            # Update schema with new table
            parent_schema[table_key] = {
                'columns': columns,
                'row_count': len(result_rows)
            }
        
        # Save updated schema
        await conn.execute("""
            INSERT INTO dsa_core.commit_schemas (commit_id, schema_definition)
            VALUES ($1, $2::jsonb)
        """, commit_id, json.dumps(parent_schema))
        
        # Create table analysis for the new table
        if result_rows:
            # Compute basic statistics
            column_types = {}
            null_counts = {}
            sample_values = {}
            
            # Initialize from first row structure
            first_row = dict(result_rows[0])
            if 'data' in first_row and isinstance(first_row['data'], dict):
                sample_data = first_row['data']
            else:
                sample_data = first_row
            
            for col in sample_data.keys():
                column_types[col] = 'text'  # Will be refined
                null_counts[col] = 0
                sample_values[col] = []
            
            # Analyze all rows
            for row in result_rows[:100]:  # Sample first 100 rows for analysis
                row_dict = dict(row)
                if 'data' in row_dict and isinstance(row_dict['data'], dict):
                    row_data = row_dict['data']
                else:
                    row_data = row_dict
                
                for col, value in row_data.items():
                    if value is None:
                        null_counts[col] = null_counts.get(col, 0) + 1
                    else:
                        # Collect sample values
                        if col in sample_values and len(sample_values[col]) < 20:
                            sample_values[col].append(value)
                        
                        # Infer type
                        if col in column_types and column_types[col] == 'text':
                            if isinstance(value, bool):
                                column_types[col] = 'boolean'
                            elif isinstance(value, int):
                                column_types[col] = 'integer'
                            elif isinstance(value, float):
                                column_types[col] = 'float'
            
            # Create analysis data
            analysis_data = {
                'total_rows': len(result_rows),
                'columns': list(column_types.keys()),
                'column_types': column_types,
                'null_counts': null_counts,
                'sample_values': sample_values,
                'statistics': {
                    'table_key': table_key,
                    'created_by': 'sql_transform',
                    'transform_message': message
                }
            }
            
            # Insert table analysis
            await conn.execute("""
                INSERT INTO dsa_core.table_analysis (commit_id, table_key, analysis)
                VALUES ($1, $2, $3::jsonb)
                ON CONFLICT (commit_id, table_key) DO UPDATE 
                SET analysis = EXCLUDED.analysis
            """, commit_id, table_key, json.dumps(analysis_data))
        
        return commit_id
    
    async def _create_output_branch(self, conn: asyncpg.Connection, dataset_id: int, branch_name: str, commit_id: str) -> None:
        """Create a new branch pointing to the output commit."""
        await conn.execute("""
            INSERT INTO dsa_core.refs (dataset_id, name, commit_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (dataset_id, name) DO UPDATE SET commit_id = EXCLUDED.commit_id
        """, dataset_id, branch_name, commit_id)