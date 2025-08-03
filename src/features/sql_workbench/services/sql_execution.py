"""Implementation of SQL execution services."""

import re
import json
import hashlib
import time
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
import asyncpg
from .sql_validator import SqlValidator, ValidationLevel

from dataclasses import dataclass
from typing import Optional
from src.core.models import TableSchema

# Data classes for SQL execution
@dataclass
class SqlSource:
    """Represents a source table for SQL execution."""
    dataset_id: int
    ref: str
    table_key: str
    alias: str

@dataclass
class SqlTarget:
    """Represents the target for SQL results."""
    dataset_id: int
    ref: str
    table_key: str
    message: str
    output_branch_name: Optional[str] = None
    expected_head_commit_id: Optional[str] = None

@dataclass
class SqlExecutionPlan:
    """Execution plan for SQL transformation."""
    sources: List[SqlSource]
    sql_query: str
    target: SqlTarget
    estimated_rows: Optional[int] = None
    estimated_memory_mb: Optional[float] = None
    optimization_hints: Optional[List[str]] = None

@dataclass
class SqlExecutionResult:
    """Result of SQL execution."""
    new_commit_id: str
    rows_processed: int
    execution_time_ms: int
    table_key: str
    output_branch_name: str
    memory_used_mb: Optional[float] = None
    optimization_applied: Optional[List[str]] = None

# TableSchema now imported from src.core.models
from src.infrastructure.postgres.database import DatabasePool


class SqlValidationService:
    """Service for validating SQL queries using unified validator."""
    
    def __init__(self):
        self._validator = SqlValidator()
    
    async def validate_query(
        self,
        sql: str,
        sources: List[SqlSource]
    ) -> Tuple[bool, List[str]]:
        """Validate SQL syntax and semantic correctness."""
        # Convert sources to format expected by validator
        source_configs = [{'alias': source.alias} for source in sources]
        
        # Use unified validator
        result = await self._validator.validate(
            sql=sql,
            sources=source_configs,
            level=ValidationLevel.ALL
        )
        
        return result.is_valid, result.errors
    
    async def estimate_resource_usage(
        self,
        sql: str,
        sources: List[SqlSource],
        table_reader
    ) -> Dict[str, Any]:
        """Estimate memory and time requirements for the query."""
        # Use validator's resource estimation
        estimate = self._validator.get_resource_estimate(sql)
        
        # Add source-specific estimations
        # Basic estimation - assume 10000 rows per source
        estimated_rows = len(sources) * 10000
        
        # Adjust based on complexity
        if estimate['complexity'] == 'high':
            estimated_rows *= 2
        elif estimate['complexity'] == 'medium':
            estimated_rows *= 1.5
        
        # Memory estimation (very rough)
        bytes_per_row = 1000  # Assume 1KB per row average
        estimated_memory_mb = (estimated_rows * bytes_per_row) / (1024 * 1024)
        
        # Adjust memory based on validator's assessment
        if estimate['memory_usage'] == 'high':
            estimated_memory_mb *= 2
        
        # Analyze SQL for operations
        sql_upper = sql.upper()
        operations = {
            'has_join': bool(re.search(r'\bJOIN\b', sql_upper)),
            'has_order_by': bool(re.search(r'\bORDER\s+BY\b', sql_upper)),
            'has_group_by': bool(re.search(r'\bGROUP\s+BY\b', sql_upper)),
            'has_distinct': 'DISTINCT' in sql_upper,
            'has_aggregation': bool(re.search(r'\b(COUNT|SUM|AVG|MAX|MIN)\s*\(', sql_upper))
        }
        
        return {
            'estimated_rows': int(estimated_rows),
            'estimated_memory_mb': estimated_memory_mb,
            'estimated_time_ms': int(estimated_rows / 1000),  # 1ms per 1000 rows
            'complexity': estimate['complexity'],
            'estimated_runtime': estimate['estimated_runtime'],
            'recommendations': estimate['recommendations'],
            'operations': operations
        }
    
    def sanitize_query(self, sql: str) -> str:
        """Sanitize SQL query for safe execution."""
        # Remove comments
        sql = re.sub(r'--[^\n]*', '', sql)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        
        # Remove multiple semicolons (prevent multiple statements)
        sql = sql.replace(';', '')
        
        # Trim whitespace
        sql = sql.strip()
        
        return sql


class SqlExecutionService:
    """Service for executing SQL transformations."""
    
    def __init__(
        self,
        db_pool: DatabasePool,
        validation_service,
        table_reader
    ):
        self._db_pool = db_pool
        self._validation_service = validation_service
        self._table_reader = table_reader
    
    async def create_execution_plan(
        self,
        sources: List[SqlSource],
        sql: str,
        target: SqlTarget
    ) -> SqlExecutionPlan:
        """Create an optimized execution plan."""
        # Validate query first
        is_valid, errors = await self._validation_service.validate_query(sql, sources)
        if not is_valid:
            raise ValueError(f"Invalid SQL query: {', '.join(errors)}")
        
        # Sanitize query
        sanitized_sql = self._validation_service.sanitize_query(sql)
        
        # Estimate resources
        estimates = await self._validation_service.estimate_resource_usage(
            sanitized_sql, sources, self._table_reader
        )
        
        # Create plan
        return SqlExecutionPlan(
            sources=sources,
            sql_query=sanitized_sql,
            target=target,
            estimated_rows=estimates['estimated_rows'],
            estimated_memory_mb=estimates['estimated_memory_mb'],
            optimization_hints=self._generate_optimization_hints(sanitized_sql, estimates)
        )
    
    async def execute_transformation(
        self,
        plan: SqlExecutionPlan,
        job_id: str,
        user_id: int
    ) -> SqlExecutionResult:
        """Execute the SQL transformation according to the plan."""
        start_time = time.time()
        
        async with self._db_pool.acquire() as conn:
            async with conn.transaction():
                view_names = []
                try:
                    # Create temporary views for sources
                    try:
                        view_names = await self._create_source_views(
                            conn, plan.sources, job_id
                        )
                    except Exception as e:
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.error(f"Failed to create source views: {str(e)}")
                        raise Exception(f"View creation failed: {str(e)}")
                    
                    # Replace aliases with view names
                    modified_sql = self._replace_aliases_with_views(
                        plan.sql_query, view_names
                    )
                    
                    # Log the modified SQL for debugging
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.info(f"Original SQL: {plan.sql_query}")
                    logger.info(f"Modified SQL: {modified_sql}")
                    
                    # For save mode, use server-side processing
                    # This avoids loading any data into application memory
                    try:
                        new_commit_id, rows_processed = await self._create_commit_with_results_server_side(
                            conn,
                            modified_sql,
                            plan.target,
                            user_id
                        )
                    except Exception as e:
                        logger.error(f"SQL execution failed: {str(e)}")
                        logger.error(f"Failed SQL was: {modified_sql}")
                        raise Exception(f"SQL execution error: {str(e)}")
                    
                    # Update ref with optimistic locking if specified
                    await self._update_ref(
                        conn,
                        plan.target.dataset_id,
                        plan.target.ref,
                        new_commit_id,
                        plan.target.expected_head_commit_id
                    )
                    
                    # Create output branch with wkbh- prefix
                    if plan.target.output_branch_name:
                        output_branch = f"wkbh-{plan.target.output_branch_name}"
                    else:
                        # Use timestamp-based name if not specified
                        output_branch = f"wkbh-transform-{int(time.time())}"
                    
                    await self._create_branch(
                        conn,
                        plan.target.dataset_id,
                        output_branch,
                        new_commit_id
                    )
                    
                    execution_time_ms = int((time.time() - start_time) * 1000)
                    
                    return SqlExecutionResult(
                        new_commit_id=new_commit_id,
                        rows_processed=rows_processed,
                        execution_time_ms=execution_time_ms,
                        table_key='primary',  # Always use 'primary' for workbench
                        output_branch_name=output_branch
                    )
                    
                except Exception as e:
                    # Ensure we clean up views even on error
                    for _, view_name in view_names:
                        try:
                            await conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                        except:
                            pass  # Ignore cleanup errors
                    raise e
                    
                finally:
                    # Final cleanup attempt
                    for _, view_name in view_names:
                        try:
                            await conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                        except:
                            pass
    
    async def preview_results(
        self,
        sources: List[SqlSource],
        sql: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Preview transformation results without committing."""
        # Add LIMIT to query for preview
        preview_sql = f"SELECT * FROM ({sql}) AS preview LIMIT {limit}"
        
        async with self._db_pool.acquire() as conn:
            # Create temporary views
            view_names = await self._create_source_views(
                conn, sources, f"preview_{datetime.utcnow().timestamp()}"
            )
            
            try:
                # Replace aliases with view names
                modified_sql = self._replace_aliases_with_views(
                    preview_sql, view_names
                )
                
                # Execute query
                rows = await conn.fetch(modified_sql)
                
                # Convert to list of dicts
                return [dict(row) for row in rows]
                
            finally:
                # Clean up views
                for _, view_name in view_names:
                    await conn.execute(f"DROP VIEW IF EXISTS {view_name}")
    
    async def _create_source_views(
        self,
        conn: asyncpg.Connection,
        sources: List[SqlSource],
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
                source.dataset_id,
                source.ref
            )
            
            if not commit_row:
                raise ValueError(f"Ref '{source.ref}' not found")
            
            commit_id = commit_row['commit_id']
            
            # Create unique view name
            view_name = f"sql_transform_{source.alias}_{job_id.replace('-', '_')}"
            
            # Create view
            escaped_table_key = source.table_key.replace("'", "''")
            
            await conn.execute(f"""
                CREATE TEMPORARY VIEW {view_name} AS
                SELECT 
                    cr.logical_row_id,
                    r.data as data
                FROM dsa_core.commit_rows cr
                JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = '{commit_id}'
                AND cr.logical_row_id LIKE '{escaped_table_key}:%'
            """)
            
            view_names.append((source.alias, view_name))
        
        return view_names
    
    def _replace_aliases_with_views(
        self,
        sql: str,
        view_names: List[Tuple[str, str]]
    ) -> str:
        """Replace table aliases with temporary view names."""
        modified_sql = sql
        
        # Sort by length descending to avoid partial replacements
        sorted_views = sorted(view_names, key=lambda x: len(x[0]), reverse=True)
        
        for alias, view_name in sorted_views:
            # Replace alias when used as table prefix (e.g., alias.column)
            # Do this FIRST to handle explicit table references
            modified_sql = re.sub(
                rf'\b{re.escape(alias)}\.', 
                f'{view_name}.',
                modified_sql
            )
            
            # Replace alias as a standalone table reference in FROM/JOIN clauses
            # More specific patterns to avoid replacing column references
            # This handles: FROM alias, JOIN alias, etc.
            # but NOT: SELECT alias->>'field' or WHERE alias = 'value'
            modified_sql = re.sub(
                rf'(\bFROM\s+){re.escape(alias)}\b',
                rf'\1{view_name}',
                modified_sql,
                flags=re.IGNORECASE
            )
            modified_sql = re.sub(
                rf'(\bJOIN\s+){re.escape(alias)}\b',
                rf'\1{view_name}',
                modified_sql,
                flags=re.IGNORECASE
            )
            # Handle comma-separated tables: FROM table1, alias
            modified_sql = re.sub(
                rf'(,\s*){re.escape(alias)}\b(?!\s*\.|\s*->>|\s*->)',
                rf'\1{view_name}',
                modified_sql
            )
        
        return modified_sql
    
    async def _create_commit_with_results_server_side(
        self,
        conn: asyncpg.Connection,
        transformation_sql: str,
        target: SqlTarget,
        user_id: int
    ) -> tuple[str, int]:
        """Create a new commit with transformation results using server-side processing."""
        # Generate commit ID with random component to ensure uniqueness
        import uuid
        commit_id = hashlib.sha256(
            f"{target.dataset_id}:{target.message}:{datetime.utcnow().isoformat()}:{uuid.uuid4()}".encode()
        ).hexdigest()
        
        # Get parent commit
        parent_ref = await conn.fetchrow(
            "SELECT commit_id FROM dsa_core.refs WHERE dataset_id = $1 AND name = $2",
            target.dataset_id, target.ref
        )
        parent_commit_id = parent_ref['commit_id'] if parent_ref else None
        
        # Create commit
        await conn.execute(
            """
            INSERT INTO dsa_core.commits (
                commit_id, dataset_id, parent_commit_id, 
                message, author_id, committed_at
            ) VALUES ($1, $2, $3, $4, $5, $6)
            """,
            commit_id, target.dataset_id, parent_commit_id,
            target.message, user_id, datetime.utcnow()
        )
        
        # Copy existing tables except the primary table (which we're replacing)
        if parent_commit_id:
            await conn.execute("""
                INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
                SELECT $1, logical_row_id, row_hash
                FROM dsa_core.commit_rows
                WHERE commit_id = $2
                AND NOT (logical_row_id LIKE 'primary:%' OR logical_row_id LIKE 'primary\\_%')
            """, commit_id, parent_commit_id)
        
        # Use server-side processing to insert transformation results
        # This never loads data into application memory
        # Build the query dynamically but safely
        query_template = """
            WITH transformation_results AS (
                -- User's transformation SQL goes here
                {transformation_sql}
            ),
            numbered_results AS (
                SELECT 
                    row_to_json(t.*)::jsonb as result_data,
                    row_number() OVER () as row_num
                FROM transformation_results t
            ),
            prepared_rows AS (
                SELECT 
                    json_build_object(
                        'sheet_name', 'primary',
                        'row_number', row_num,
                        'data', result_data
                    )::jsonb as full_data,
                    -- For hash calculation, we only hash the actual data content
                    -- This ensures deduplication works correctly
                    encode(sha256(result_data::text::bytea), 'hex') as row_hash,
                    row_num
                FROM numbered_results
            ),
            row_inserts AS (
                INSERT INTO dsa_core.rows (row_hash, data)
                SELECT DISTINCT
                    row_hash,
                    full_data
                FROM prepared_rows
                ON CONFLICT (row_hash) DO NOTHING
            )
            INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
            SELECT 
                '{commit_id}' as commit_id,
                '{table_key}' || ':' || row_hash as logical_row_id,
                row_hash
            FROM prepared_rows
            ON CONFLICT (commit_id, logical_row_id) DO NOTHING
        """
        
        # Format the query with actual values
        # Note: commit_id is safe as it comes from our system
        # Always use 'primary' as table key for workbench transformations
        final_query = query_template.format(
            transformation_sql=transformation_sql,
            commit_id=commit_id,
            table_key='primary'  # Workbench always outputs to primary table
        )
        
        rows_processed = await conn.execute(final_query)
        
        # Get actual count of rows processed
        row_count = await conn.fetchval("""
            SELECT COUNT(*) FROM dsa_core.commit_rows 
            WHERE commit_id = $1 AND logical_row_id LIKE $2 || ':%'
        """, commit_id, 'primary')  # Always use 'primary' for workbench
        
        # Create schema for the workbench output
        # First, get a sample row to extract column names
        sample_row = await conn.fetchrow("""
            SELECT r.data
            FROM dsa_core.commit_rows cr
            JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
            WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE 'primary:%'
            LIMIT 1
        """, commit_id)
        
        columns_list = []
        if sample_row and sample_row['data']:
            data = sample_row['data']
            if isinstance(data, str):
                data = json.loads(data)
            
            # Extract actual data from standardized format
            if isinstance(data, dict) and 'data' in data:
                actual_data = data['data']
                if isinstance(actual_data, dict):
                    columns_list = list(actual_data.keys())
        
        # Create schema with 'primary' table
        schema = {
            'primary': {
                'columns': columns_list
            }
        }
        
        # Insert the schema
        await conn.execute("""
            INSERT INTO dsa_core.commit_schemas (commit_id, schema_definition)
            VALUES ($1, $2)
            ON CONFLICT (commit_id) DO UPDATE SET schema_definition = EXCLUDED.schema_definition
        """, commit_id, json.dumps(schema))
        
        return commit_id, row_count or 0
    
    async def _update_ref(
        self,
        conn: asyncpg.Connection,
        dataset_id: int,
        ref_name: str,
        commit_id: str,
        expected_head_commit_id: str = None
    ) -> None:
        """Update ref to point to new commit with optional optimistic locking."""
        if expected_head_commit_id:
            # Use optimistic locking to prevent race conditions
            result = await conn.execute(
                """
                UPDATE dsa_core.refs 
                SET commit_id = $1
                WHERE dataset_id = $2 AND name = $3 AND commit_id = $4
                """,
                commit_id, dataset_id, ref_name, expected_head_commit_id
            )
            if result == "UPDATE 0":
                raise Exception(
                    f"Concurrent update detected: ref '{ref_name}' was updated by another transaction. "
                    "Expected commit {expected_head_commit_id} but ref has moved. "
                    "Please retry your transformation with the latest commit."
                )
        else:
            # No optimistic locking - last write wins
            await conn.execute(
                """
                UPDATE dsa_core.refs 
                SET commit_id = $1
                WHERE dataset_id = $2 AND name = $3
                """,
                commit_id, dataset_id, ref_name
            )
    
    async def _create_branch(
        self,
        conn: asyncpg.Connection,
        dataset_id: int,
        branch_name: str,
        commit_id: str
    ) -> None:
        """Create or update a branch."""
        await conn.execute(
            """
            INSERT INTO dsa_core.refs (dataset_id, name, commit_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (dataset_id, name) 
            DO UPDATE SET commit_id = EXCLUDED.commit_id
            """,
            dataset_id, branch_name, commit_id
        )
    
    def _generate_optimization_hints(
        self,
        sql: str,
        estimates: Dict[str, Any]
    ) -> List[str]:
        """Generate optimization hints based on query analysis."""
        hints = []
        
        if estimates['estimated_memory_mb'] > 1000:
            hints.append("Consider adding filters to reduce data volume")
        
        if estimates['operations']['has_join'] and estimates['estimated_rows'] > 100000:
            hints.append("Consider indexing join columns")
        
        if estimates['operations']['has_order_by']:
            hints.append("Consider limiting results if full ordering is not needed")
        
        return hints

