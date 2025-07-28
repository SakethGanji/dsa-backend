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
        
        return {
            'estimated_rows': int(estimated_rows),
            'estimated_memory_mb': estimated_memory_mb,
            'estimated_time_ms': int(estimated_rows / 1000),  # 1ms per 1000 rows
            'complexity': estimate['complexity'],
            'estimated_runtime': estimate['estimated_runtime'],
            'recommendations': estimate['recommendations']
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
                # Create temporary views for sources
                view_names = await self._create_source_views(
                    conn, plan.sources, job_id
                )
                
                try:
                    # Replace aliases with view names
                    modified_sql = self._replace_aliases_with_views(
                        plan.sql_query, view_names
                    )
                    
                    # Execute query
                    result_rows = await conn.fetch(modified_sql)
                    
                    # Create new commit with results
                    new_commit_id = await self._create_commit_with_results(
                        conn,
                        result_rows,
                        plan.target,
                        user_id
                    )
                    
                    # Update ref
                    await self._update_ref(
                        conn,
                        plan.target.dataset_id,
                        plan.target.ref,
                        new_commit_id
                    )
                    
                    # Create output branch if specified
                    output_branch = plan.target.output_branch_name or new_commit_id
                    await self._create_branch(
                        conn,
                        plan.target.dataset_id,
                        output_branch,
                        new_commit_id
                    )
                    
                    execution_time_ms = int((time.time() - start_time) * 1000)
                    
                    return SqlExecutionResult(
                        new_commit_id=new_commit_id,
                        rows_processed=len(result_rows),
                        execution_time_ms=execution_time_ms,
                        table_key=plan.target.table_key,
                        output_branch_name=output_branch
                    )
                    
                finally:
                    # Clean up views
                    for _, view_name in view_names:
                        await conn.execute(f"DROP VIEW IF EXISTS {view_name}")
    
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
                    r.data->'data' as data
                FROM dsa_core.commit_rows cr
                JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = '{commit_id}'
                AND (cr.logical_row_id LIKE '{escaped_table_key}:%' 
                     OR cr.logical_row_id LIKE '{escaped_table_key}\\_%')
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
            # Replace patterns
            patterns = [
                (f" {alias} ", f" {view_name} "),
                (f" {alias}.", f" {view_name}."),
                (f"FROM {alias}", f"FROM {view_name}"),
                (f"JOIN {alias}", f"JOIN {view_name}"),
                (f"({alias})", f"({view_name})"),
            ]
            
            for pattern, replacement in patterns:
                modified_sql = modified_sql.replace(pattern, replacement)
            
            # Handle SELECT * case
            if f"SELECT * FROM {view_name}" in modified_sql:
                modified_sql = modified_sql.replace("SELECT *", "SELECT data")
        
        return modified_sql
    
    async def _create_commit_with_results(
        self,
        conn: asyncpg.Connection,
        result_rows: List[asyncpg.Record],
        target: SqlTarget,
        user_id: int
    ) -> str:
        """Create a new commit with transformation results."""
        # Generate commit ID
        commit_id = hashlib.sha256(
            f"{target.dataset_id}:{target.message}:{datetime.utcnow().isoformat()}".encode()
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
        
        # Copy existing tables except the target
        if parent_commit_id:
            await conn.execute("""
                INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
                SELECT $1, logical_row_id, row_hash
                FROM dsa_core.commit_rows
                WHERE commit_id = $2
                AND NOT (logical_row_id LIKE $3 || ':%' OR logical_row_id LIKE $3 || '\\_%')
            """, commit_id, parent_commit_id, target.table_key)
        
        # Process result rows
        for idx, row in enumerate(result_rows):
            row_dict = dict(row)
            
            # Handle data structure
            if 'data' in row_dict and len(row_dict) == 1:
                inner_data = row_dict['data']
                if isinstance(inner_data, str):
                    inner_data = json.loads(inner_data)
            else:
                inner_data = row_dict
            
            # Create row data
            row_data = {
                "sheet_name": target.table_key,
                "row_number": idx + 1,
                "data": inner_data
            }
            row_json = json.dumps(row_data, default=str)
            
            # Calculate hash
            data_json = json.dumps(inner_data, sort_keys=True, separators=(',', ':'), default=str)
            row_hash = hashlib.sha256(data_json.encode()).hexdigest()
            
            # Insert row
            await conn.execute(
                """
                INSERT INTO dsa_core.rows (row_hash, data)
                VALUES ($1, $2::jsonb)
                ON CONFLICT (row_hash) DO NOTHING
                """,
                row_hash, row_json
            )
            
            # Link to commit
            logical_row_id = f"{target.table_key}:{row_hash}"
            await conn.execute(
                """
                INSERT INTO dsa_core.commit_rows (
                    commit_id, logical_row_id, row_hash
                ) VALUES ($1, $2, $3)
                """,
                commit_id, logical_row_id, row_hash
            )
        
        return commit_id
    
    async def _update_ref(
        self,
        conn: asyncpg.Connection,
        dataset_id: int,
        ref_name: str,
        commit_id: str
    ) -> None:
        """Update ref to point to new commit."""
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

