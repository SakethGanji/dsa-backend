"""Service for SQL workbench operations including transformations and previews."""
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

from src.infrastructure.postgres.table_reader import PostgresTableReader
from src.infrastructure.postgres.uow import PostgresUnitOfWork


@dataclass
class TransformationResult:
    """Result of a transformation operation."""
    transformation_id: str
    dataset_id: str
    status: str
    created_at: datetime
    sql: str


@dataclass
class ValidationResult:
    """Result of SQL validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    referenced_tables: List[str]


@dataclass
class PreviewResult:
    """Result of transformation preview."""
    data: List[Dict[str, Any]]
    schema: Dict[str, Any]
    row_count: int
    execution_time_ms: float


class WorkbenchService:
    """Service for SQL workbench operations."""
    
    def __init__(
        self,
        table_reader: PostgresTableReader = None,
        sql_validator = None,
        uow: PostgresUnitOfWork = None,
        db_pool = None
    ):
        self._table_reader = table_reader
        self._sql_validator = sql_validator
        self._uow = uow
        self._db_pool = db_pool
        self._temp_views: Dict[str, str] = {}
    
    async def _execute_sql_with_sources(
        self, 
        sql: str, 
        sources: List[Dict[str, Any]],
        db_pool = None
    ) -> Dict[str, Any]:
        """Execute SQL query with source table CTEs."""
        pool = db_pool or self._db_pool
        if not pool:
            raise ValueError("Database pool not available")
        
        async with pool.acquire() as conn:
            try:
                # Build CTEs for each source table
                cte_parts = []
                for source in sources:
                    cte_sql = f"""
                    {source['alias']} AS (
                        SELECT 
                            (r.data->>'data')::jsonb as data,
                            cr.logical_row_id
                        FROM dsa_core.commit_rows cr
                        JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                        WHERE cr.commit_id = '{source['commit_id']}'
                        AND cr.logical_row_id LIKE '{source['table_key']}:%'
                    )"""
                    cte_parts.append(cte_sql)
                
                # Build the full query with CTEs
                full_query = f"""
                WITH {','.join(cte_parts)}
                {sql}
                """
                
                # Execute the query
                rows = await conn.fetch(full_query)
                
                # Convert to the expected format
                if rows:
                    # Get column names from the first row
                    columns = list(rows[0].keys())
                    result_rows = [list(row.values()) for row in rows]
                    
                    return {
                        'rows': result_rows,
                        'columns': columns
                    }
                else:
                    return {
                        'rows': [],
                        'columns': []
                    }
            except Exception as e:
                raise ValueError(f"SQL execution failed: {str(e)}")
    
    async def create_transformation(
        self,
        dataset_id: str,
        sql: str,
        name: Optional[str] = None,
        description: Optional[str] = None
    ) -> TransformationResult:
        """Create a new transformation."""
        # Validate SQL first
        validation = await self.validate_transformation(sql)
        if not validation.is_valid:
            raise ValueError(f"Invalid SQL: {', '.join(validation.errors)}")
        
        # Create transformation record
        transformation_id = str(uuid.uuid4())
        transformation = TransformationResult(
            transformation_id=transformation_id,
            dataset_id=dataset_id,
            status='created',
            created_at=datetime.utcnow(),
            sql=sql
        )
        
        # Store transformation (would normally persist to database)
        # For now, just return the result
        return transformation
    
    async def validate_transformation(self, sql: str) -> ValidationResult:
        """Validate SQL transformation."""
        errors = []
        warnings = []
        referenced_tables = []
        
        # Basic SQL validation
        sql_lower = sql.lower()
        
        # Check for dangerous operations
        dangerous_keywords = ['drop', 'delete', 'truncate', 'alter', 'create', 'insert', 'update']
        for keyword in dangerous_keywords:
            if re.search(r'\b' + keyword + r'\b', sql_lower):
                errors.append(f"Dangerous operation '{keyword}' not allowed in transformations")
        
        # Extract referenced tables
        table_pattern = r'from\s+(\w+)|join\s+(\w+)'
        matches = re.findall(table_pattern, sql_lower)
        for match in matches:
            table_name = match[0] or match[1]
            if table_name and table_name not in referenced_tables:
                referenced_tables.append(table_name)
        
        # Use SQL validator service for deeper validation if available
        if self._sql_validator:
            try:
                await self._sql_validator.validate_query(sql)
            except Exception as e:
                errors.append(str(e))
        
        # Add warnings for common issues
        if 'select *' in sql_lower:
            warnings.append("Using SELECT * may impact performance")
        
        if not sql_lower.strip().endswith(';'):
            warnings.append("SQL statement should end with semicolon")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            referenced_tables=referenced_tables
        )
    
    async def preview_transformation(
        self,
        dataset_id: str,
        commit_id: str,
        sql: str,
        limit: int = 100
    ) -> PreviewResult:
        """Preview transformation results."""
        import time
        start_time = time.time()
        
        # Validate SQL first
        validation = await self.validate_transformation(sql)
        if not validation.is_valid:
            raise ValueError(f"Invalid SQL: {', '.join(validation.errors)}")
        
        # For now, return a simple error since we need source information
        # This method seems to be for a different use case than the preview_sql endpoint
        raise NotImplementedError(
            "This method requires refactoring to work with the new architecture. "
            "Use the /workbench/sql-preview endpoint instead."
        )
    
    async def apply_transformation(
        self,
        dataset_id: str,
        transformation_id: str,
        target_table_name: str
    ) -> str:
        """Apply a transformation and create a job."""
        # Get transformation (would normally fetch from database)
        # For now, we'll create a job directly
        
        if self._uow:
            job = await self._uow.jobs.create_job({
                'type': 'sql_transformation',
                'dataset_id': dataset_id,
                'parameters': {
                    'transformation_id': transformation_id,
                    'target_table': target_table_name
                },
                'created_by': 'system'  # Would come from context
            })
            return job.id
        else:
            raise ValueError("Unit of work not configured")
    
    def _schema_to_dict(self, schema) -> Dict[str, Any]:
        """Convert schema object to dictionary."""
        return {
            'columns': [
                {
                    'name': col.name,
                    'type': col.data_type,
                    'nullable': col.is_nullable
                }
                for col in schema.columns
            ]
        }