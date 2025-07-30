"""Utility class for executing SQL queries with data sources."""
from typing import List, Dict, Any, Optional


class SqlExecutor:
    """Executes SQL queries with source table CTEs."""
    
    async def execute_sql_with_sources(
        self, 
        sql: str, 
        sources: List[Dict[str, Any]],
        db_pool
    ) -> Dict[str, Any]:
        """
        Execute SQL query with source table CTEs.
        
        Args:
            sql: The SQL query to execute
            sources: List of source table configurations, each containing:
                - alias: Table alias to use in SQL
                - dataset_id: Dataset ID
                - commit_id: Commit ID to read from
                - table_key: Table key prefix
            db_pool: Database connection pool
            
        Returns:
            Dictionary with 'rows' and 'columns' keys
        """
        if not db_pool:
            raise ValueError("Database pool not available")
        
        async with db_pool.acquire() as conn:
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