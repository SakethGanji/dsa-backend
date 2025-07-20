"""Service for SQL workbench operations including transformations and previews."""
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

from src.core.abstractions.services import IWorkbenchService, ISqlValidationService
from src.core.abstractions.repositories import ITableReader
from src.core.abstractions.uow import IUnitOfWork


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


class WorkbenchService(IWorkbenchService):
    """Service for SQL workbench operations."""
    
    def __init__(
        self,
        table_reader: ITableReader,
        sql_validator: ISqlValidationService,
        uow: IUnitOfWork
    ):
        self._table_reader = table_reader
        self._sql_validator = sql_validator
        self._uow = uow
        self._temp_views: Dict[str, str] = {}
    
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
        
        # Use SQL validator service for deeper validation
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
        
        # Create temporary view name
        temp_view_name = f"temp_preview_{uuid.uuid4().hex[:8]}"
        
        try:
            # Create view for the transformation
            create_view_sql = f"CREATE TEMPORARY VIEW {temp_view_name} AS {sql}"
            await self._table_reader.execute_query(
                dataset_id=dataset_id,
                commit_id=commit_id,
                query=create_view_sql
            )
            
            # Get schema of the view
            schema = await self._table_reader.get_table_schema(
                dataset_id=dataset_id,
                commit_id=commit_id,
                table_name=temp_view_name
            )
            
            # Get preview data
            preview_sql = f"SELECT * FROM {temp_view_name} LIMIT {limit}"
            result = await self._table_reader.execute_query(
                dataset_id=dataset_id,
                commit_id=commit_id,
                query=preview_sql
            )
            
            # Convert rows to dictionaries
            column_names = [col.name for col in schema.columns]
            data = []
            for row in result.rows:
                row_dict = dict(zip(column_names, row))
                data.append(row_dict)
            
            # Get total row count
            count_sql = f"SELECT COUNT(*) FROM {temp_view_name}"
            count_result = await self._table_reader.execute_query(
                dataset_id=dataset_id,
                commit_id=commit_id,
                query=count_sql
            )
            row_count = count_result.rows[0][0] if count_result.rows else 0
            
            execution_time_ms = (time.time() - start_time) * 1000
            
            return PreviewResult(
                data=data,
                schema=self._schema_to_dict(schema),
                row_count=row_count,
                execution_time_ms=execution_time_ms
            )
            
        finally:
            # Clean up temporary view
            try:
                drop_view_sql = f"DROP VIEW IF EXISTS {temp_view_name}"
                await self._table_reader.execute_query(
                    dataset_id=dataset_id,
                    commit_id=commit_id,
                    query=drop_view_sql
                )
            except:
                pass  # Best effort cleanup
    
    async def apply_transformation(
        self,
        dataset_id: str,
        transformation_id: str,
        target_table_name: str
    ) -> str:
        """Apply a transformation and create a job."""
        # Get transformation (would normally fetch from database)
        # For now, we'll create a job directly
        
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