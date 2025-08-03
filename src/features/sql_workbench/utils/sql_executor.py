"""Utility class for executing SQL queries with data sources."""
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


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
                            r.data as data,
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
    
    async def execute_sql_with_sampled_sources(
        self, 
        sql: str, 
        sources: List[Dict[str, Any]],
        db_pool,
        sample_percent: float = 1.0
    ) -> Dict[str, Any]:
        """
        Execute SQL query with sampled source table CTEs for quick preview.
        Uses multi-CTE approach to filter BEFORE joins for performance.
        
        Args:
            sql: The SQL query to execute
            sources: List of source table configurations
            db_pool: Database connection pool
            sample_percent: Percentage of rows to sample (0.0-100.0)
            
        Returns:
            Dictionary with 'rows' and 'columns' keys
        """
        if not db_pool:
            raise ValueError("Database pool not available")
        
        if sample_percent <= 0 or sample_percent > 100:
            raise ValueError("sample_percent must be between 0 and 100")
        
        async with db_pool.acquire() as conn:
            try:
                # Build multi-level CTEs with sampling
                cte_parts = []
                sample_ratio = sample_percent / 100.0
                
                for source in sources:
                    # Validate alias to prevent SQL injection
                    if not source['alias'].isidentifier():
                        raise ValueError(f"Invalid alias: {source['alias']}")
                    
                    # First CTE: Filter commit rows with random sampling
                    filtered_cte = f"""
                    __{source['alias']}_filtered AS (
                        SELECT logical_row_id, row_hash
                        FROM dsa_core.commit_rows
                        WHERE commit_id = '{source['commit_id']}'
                        AND logical_row_id LIKE '{source['table_key']}:%'
                        AND random() < {sample_ratio}
                    )"""
                    
                    # Second CTE: Join only the sampled rows
                    data_cte = f"""
                    {source['alias']} AS (
                        SELECT 
                            r.data as data,
                            f.logical_row_id
                        FROM __{source['alias']}_filtered f
                        JOIN dsa_core.rows r ON f.row_hash = r.row_hash
                    )"""
                    
                    cte_parts.extend([filtered_cte, data_cte])
                
                # Build the full query with multi-level CTEs
                cte_string = ',\n'.join(cte_parts)
                full_query = f"""
                -- Quick preview with {sample_percent}% random sample
                WITH {cte_string}
                {sql}
                """
                
                logger.info(f"Executing quick preview with {sample_percent}% sampling")
                
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
                raise ValueError(f"SQL execution with sampling failed: {str(e)}")