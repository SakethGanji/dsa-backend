"""Optimization utilities for SQL workbench preview mode."""

import re
from typing import Tuple

def optimize_preview_query(user_sql: str, limit: int, offset: int) -> Tuple[str, bool]:
    """
    Optimize query for preview mode to avoid running expensive operations on full dataset.
    
    Returns:
        (optimized_sql, is_modified): The optimized query and whether it was modified
    """
    # Remove comments and normalize whitespace
    clean_sql = re.sub(r'--.*$', '', user_sql, flags=re.MULTILINE)
    clean_sql = re.sub(r'/\*.*?\*/', '', clean_sql, flags=re.DOTALL)
    
    # Check if query already has LIMIT
    has_limit = re.search(r'\bLIMIT\s+\d+', clean_sql, re.IGNORECASE)
    
    # Check for complex operations that would benefit from early limiting
    has_aggregation = re.search(r'\b(GROUP\s+BY|SUM|COUNT|AVG|MAX|MIN)\b', clean_sql, re.IGNORECASE)
    has_order_by = re.search(r'\bORDER\s+BY\b', clean_sql, re.IGNORECASE)
    has_distinct = re.search(r'\bDISTINCT\b', clean_sql, re.IGNORECASE)
    
    # Case 1: Simple SELECT without aggregation - inject LIMIT early
    if not has_limit and not has_aggregation and not has_distinct:
        if offset > 0:
            # Need to wrap for offset
            return f"""
            SELECT * FROM (
                {user_sql}
            ) AS preview_result
            LIMIT {limit}
            OFFSET {offset}
            """, True
        else:
            # Can inject LIMIT directly for better performance
            if has_order_by:
                # Keep original ordering
                return f"{user_sql}\nLIMIT {limit}", True
            else:
                # Add LIMIT to the end
                return f"{user_sql}\nLIMIT {limit}", True
    
    # Case 2: Query with aggregation or DISTINCT - wrap to ensure correctness
    # This is the current behavior and safest option
    return f"""
    SELECT * FROM (
        {user_sql}
    ) AS query_result
    LIMIT {limit}
    OFFSET {offset}
    """, True


def create_approximate_preview_query(user_sql: str, sources: list[dict], limit: int, sample_percent: int = 1) -> str:
    """
    Create an approximate preview using proper multi-CTE sampling.
    This gives fast approximate results by filtering BEFORE the expensive join.
    
    WARNING: Results are APPROXIMATE and should only be used for testing query logic.
    """
    # Build CTEs with sampling applied BEFORE the join
    cte_parts = []
    sample_ratio = sample_percent / 100.0
    
    for source in sources:
        # First CTE: Filter commit rows with random sampling
        filtered_cte = f"""
        __{source['alias']}_filtered AS (
            SELECT logical_row_id, row_hash
            FROM dsa_core.commit_rows
            WHERE commit_id = '{source['commit_id']}'
            AND logical_row_id LIKE '{source['table_key']}:%'
            AND random() < {sample_ratio}  -- Sample BEFORE the join!
        )"""
        
        # Second CTE: Join only the sampled rows
        data_cte = f"""
        {source['alias']} AS (
            SELECT 
                (r.data->>'data')::jsonb as data,
                f.logical_row_id
            FROM __{source['alias']}_filtered f
            JOIN dsa_core.rows r ON f.row_hash = r.row_hash
        )"""
        
        cte_parts.extend([filtered_cte, data_cte])
    
    # Build the full query with multi-level CTEs
    cte_string = ',\n'.join(cte_parts)
    full_query = f"""
    -- APPROXIMATE RESULTS: Using {sample_percent}% random sample
    -- Sampling happens BEFORE joins for performance
    WITH {cte_string}
    SELECT * FROM (
        {user_sql}
    ) AS sampled_result
    LIMIT {limit}
    """
    
    return full_query


def suggest_query_optimization(user_sql: str) -> list[str]:
    """
    Suggest optimizations to the user for better preview performance.
    """
    suggestions = []
    
    # Check for SELECT *
    if re.search(r'SELECT\s+\*', user_sql, re.IGNORECASE):
        suggestions.append("Consider selecting only needed columns instead of SELECT *")
    
    # Check for missing WHERE clause on large operations  
    has_where = re.search(r'\bWHERE\b', user_sql, re.IGNORECASE)
    has_aggregation = re.search(r'\b(GROUP\s+BY|SUM|COUNT|AVG)\b', user_sql, re.IGNORECASE)
    
    if has_aggregation and not has_where:
        suggestions.append("Consider adding a WHERE clause to limit data before aggregation")
    
    # Check for multiple JOINs
    join_count = len(re.findall(r'\bJOIN\b', user_sql, re.IGNORECASE))
    if join_count > 3:
        suggestions.append(f"Query has {join_count} JOINs - consider filtering early to improve preview performance")
    
    return suggestions