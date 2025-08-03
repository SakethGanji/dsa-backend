"""Enhanced SQL Executor with automatic field expansion support."""
from typing import List, Dict, Any, Optional
import logging
import json

from .sql_executor import SqlExecutor

logger = logging.getLogger(__name__)


class EnhancedSqlExecutor(SqlExecutor):
    """SQL Executor with automatic JSONB field expansion."""
    
    async def execute_sql_with_sources(
        self, 
        sql: str, 
        sources: List[Dict[str, Any]],
        db_pool,
        auto_expand: bool = False
    ) -> Dict[str, Any]:
        """
        Execute SQL query with optional automatic field expansion.
        
        Args:
            sql: The SQL query to execute
            sources: List of source table configurations
            db_pool: Database connection pool
            auto_expand: If True, automatically expand JSONB fields into columns
            
        Returns:
            Dictionary with 'rows' and 'columns' keys
        """
        if not auto_expand:
            # Use parent class implementation for backward compatibility
            return await super().execute_sql_with_sources(sql, sources, db_pool)
        
        if not db_pool:
            raise ValueError("Database pool not available")
        
        async with db_pool.acquire() as conn:
            try:
                # Build CTEs with field expansion
                cte_parts = []
                for source in sources:
                    # First, sample the data to get field names
                    sample_row = await conn.fetchrow(f"""
                        SELECT r.data
                        FROM dsa_core.commit_rows cr
                        JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                        WHERE cr.commit_id = $1
                        AND cr.logical_row_id LIKE $2
                        LIMIT 1
                    """, source['commit_id'], f"{source['table_key']}:%")
                    
                    if sample_row and sample_row['data']:
                        # Extract field names from JSONB
                        data = sample_row['data']
                        if isinstance(data, str):
                            data = json.loads(data)
                        
                        fields = list(data.keys()) if isinstance(data, dict) else []
                        
                        # Build column expressions with type inference
                        column_exprs = ['cr.logical_row_id']
                        
                        for field in fields:
                            sample_value = data.get(field)
                            
                            # Infer type and create appropriate cast
                            if isinstance(sample_value, int):
                                column_exprs.append(f"(r.data->>'{field}')::integer as {field}")
                            elif isinstance(sample_value, float):
                                column_exprs.append(f"(r.data->>'{field}')::numeric as {field}")
                            elif isinstance(sample_value, bool):
                                column_exprs.append(f"(r.data->>'{field}')::boolean as {field}")
                            else:
                                # Default to text
                                column_exprs.append(f"r.data->>'{field}' as {field}")
                        
                        # Also include raw data column
                        column_exprs.append('r.data as data')
                        
                        # Create CTE with expanded fields
                        cte_sql = f"""
                        {source['alias']} AS (
                            SELECT 
                                {','.join(column_exprs)}
                            FROM dsa_core.commit_rows cr
                            JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                            WHERE cr.commit_id = '{source['commit_id']}'
                            AND cr.logical_row_id LIKE '{source['table_key']}:%'
                        )"""
                    else:
                        # Fallback to traditional mode if no sample data
                        cte_sql = f"""
                        {source['alias']} AS (
                            SELECT 
                                cr.logical_row_id,
                                r.data as data
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
                
                logger.debug(f"Executing expanded query: {full_query[:200]}...")
                
                # Execute the query
                rows = await conn.fetch(full_query)
                
                # Convert to the expected format
                if rows:
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
                logger.error(f"SQL execution with field expansion failed: {str(e)}")
                raise ValueError(f"SQL execution failed: {str(e)}")