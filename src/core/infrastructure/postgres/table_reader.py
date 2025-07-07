"""PostgreSQL implementation of ITableReader."""

from typing import List, Dict, Any, Optional, AsyncGenerator
import json
import re
from asyncpg import Connection
from ...services.interfaces import ITableReader


class PostgresTableReader(ITableReader):
    """PostgreSQL implementation for reading table data from commits."""
    
    def __init__(self, connection: Connection):
        self._conn = connection
    
    async def list_table_keys(self, commit_id: str) -> List[str]:
        """List all available table keys for a given commit."""
        # First try to get table keys from schema
        schema_query = """
            SELECT schema_definition
            FROM dsa_core.commit_schemas
            WHERE commit_id = $1
        """
        schema_result = await self._conn.fetchval(schema_query, commit_id)
        
        if schema_result:
            # Extract table keys from schema
            if isinstance(schema_result, str):
                schema_result = json.loads(schema_result)
            return list(schema_result.keys())
        
        # Fallback: try to extract from logical_row_id
        query = """
            SELECT DISTINCT 
                CASE 
                    WHEN logical_row_id LIKE '%:%' THEN SPLIT_PART(logical_row_id, ':', 1)
                    ELSE REGEXP_REPLACE(logical_row_id, '_[0-9]+$', '')
                END AS table_key
            FROM dsa_core.commit_rows
            WHERE commit_id = $1
            ORDER BY table_key
        """
        rows = await self._conn.fetch(query, commit_id)
        return [row['table_key'] for row in rows if row['table_key']]
    
    async def get_table_schema(self, commit_id: str, table_key: str) -> Optional[Dict[str, Any]]:
        """Get the schema for a specific table within a commit."""
        query = """
            SELECT schema_definition -> $2 AS table_schema
            FROM dsa_core.commit_schemas
            WHERE commit_id = $1
        """
        result = await self._conn.fetchval(query, commit_id, table_key)
        return result if result else None
    
    async def get_table_statistics(self, commit_id: str, table_key: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific table within a commit."""
        query = """
            SELECT statistics -> $2 AS table_stats
            FROM dsa_core.commit_statistics
            WHERE commit_id = $1
        """
        result = await self._conn.fetchval(query, commit_id, table_key)
        return result if result else None
    
    async def get_table_data(
        self,
        commit_id: str,
        table_key: str,
        offset: int = 0,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get paginated data for a specific table."""
        # Handle both formats: "table_key:row_idx" and "table_key_row_idx"
        if ':' in table_key:
            pattern = f"{table_key}:%"
        else:
            pattern = f"{table_key}_%"
        
        # Also check if data contains sheet_name matching table_key
        if limit is None:
            # Get all rows (use with caution for large datasets)
            query = """
                SELECT r.data, cr.logical_row_id
                FROM dsa_core.commit_rows cr
                JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = $1 
                AND (cr.logical_row_id LIKE $2 OR r.data->>'sheet_name' = $3)
                ORDER BY cr.logical_row_id
                OFFSET $4
            """
            rows = await self._conn.fetch(query, commit_id, pattern, table_key, offset)
        else:
            # Get paginated rows
            query = """
                SELECT r.data, cr.logical_row_id
                FROM dsa_core.commit_rows cr
                JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = $1 
                AND (cr.logical_row_id LIKE $2 OR r.data->>'sheet_name' = $3)
                ORDER BY cr.logical_row_id
                OFFSET $4 LIMIT $5
            """
            rows = await self._conn.fetch(query, commit_id, pattern, table_key, offset, limit)
        
        # Parse data and include row index
        result = []
        for row in rows:
            # Parse JSON data if needed
            data = row['data']
            if isinstance(data, str):
                data = json.loads(data)
            
            # Handle nested data structure
            if 'data' in data and isinstance(data['data'], dict):
                # Extract the actual data from nested structure
                actual_data = data['data']
                result.append({
                    '_logical_row_id': row['logical_row_id'],
                    **actual_data
                })
            else:
                # Add data as-is
                result.append({
                    '_logical_row_id': row['logical_row_id'],
                    **data
                })
        
        return result
    
    async def get_table_data_stream(
        self,
        commit_id: str,
        table_key: str,
        batch_size: int = 1000
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Stream data for a specific table in batches."""
        pattern = f"{table_key}:%"
        offset = 0
        
        while True:
            # Fetch a batch of rows
            query = """
                SELECT r.data, cr.logical_row_id
                FROM dsa_core.commit_rows cr
                JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE $2
                ORDER BY cr.logical_row_id
                OFFSET $3 LIMIT $4
            """
            rows = await self._conn.fetch(query, commit_id, pattern, offset, batch_size)
            
            # If no more rows, stop
            if not rows:
                break
            
            # Process batch
            batch = []
            for row in rows:
                # Extract row index from logical_row_id
                _, row_idx = row['logical_row_id'].split(':', 1)
                
                # Parse JSON data if needed
                data = row['data']
                if isinstance(data, str):
                    data = json.loads(data)
                
                batch.append({
                    '_row_index': int(row_idx),
                    '_logical_row_id': row['logical_row_id'],
                    **data
                })
            
            yield batch
            
            # Move to next batch
            offset += len(rows)
            
            # If we got less than batch_size, we're done
            if len(rows) < batch_size:
                break
    
    async def count_table_rows(self, commit_id: str, table_key: str) -> int:
        """Get the total row count for a specific table."""
        # Handle both formats: "table_key:row_idx" and "table_key_row_idx"
        if ':' in table_key:
            pattern = f"{table_key}:%"
        else:
            pattern = f"{table_key}_%"
        
        query = """
            SELECT COUNT(*)
            FROM dsa_core.commit_rows cr
            JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
            WHERE cr.commit_id = $1 
            AND (cr.logical_row_id LIKE $2 OR r.data->>'sheet_name' = $3)
        """
        count = await self._conn.fetchval(query, commit_id, pattern, table_key)
        return count or 0
    
    async def get_column_samples(
        self, 
        commit_id: str, 
        table_key: str, 
        columns: List[str], 
        samples_per_column: int = 20
    ) -> Dict[str, List[Any]]:
        """Get unique sample values per column using SQL."""
        # Handle table key pattern
        if ':' in table_key:
            pattern = f"{table_key}:%"
        else:
            pattern = f"{table_key}_%"
        
        # Build dynamic column sampling query
        column_samples = {}
        
        for column in columns:
            # Validate column name to prevent SQL injection
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column):
                raise ValueError(f"Invalid column name: {column}")
            
            query = """
                WITH sampled_data AS (
                    SELECT DISTINCT r.data->>$3 as col_value
                    FROM dsa_core.commit_rows m
                    TABLESAMPLE SYSTEM(10)  -- 10% sample for speed
                    JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                    WHERE m.commit_id = $1 
                    AND m.logical_row_id LIKE $2
                    AND r.data ? $3  -- Column exists
                    AND r.data->>$3 IS NOT NULL  -- Not null
                    LIMIT $4 * 2  -- Get extra to ensure enough unique values
                ),
                ranked_samples AS (
                    SELECT col_value, ROW_NUMBER() OVER (ORDER BY random()) as rn
                    FROM sampled_data
                )
                SELECT col_value
                FROM ranked_samples
                WHERE rn <= $4
                ORDER BY col_value
            """
            
            rows = await self._conn.fetch(query, commit_id, pattern, column, samples_per_column)
            column_samples[column] = [row['col_value'] for row in rows]
        
        return column_samples
    
    async def get_table_sample_stream(
        self, 
        commit_id: str, 
        table_key: str,
        sample_method: str, 
        sample_params: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream sampled data based on method."""
        # Handle table key pattern
        if ':' in table_key:
            pattern = f"{table_key}:%"
        else:
            pattern = f"{table_key}_%"
        
        # Build sampling query based on method
        if sample_method == 'random':
            if sample_params.get('seed'):
                # Deterministic random sampling
                query = """
                    SELECT r.data, cr.logical_row_id
                    FROM dsa_core.commit_rows cr
                    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                    WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE $2
                    ORDER BY md5(cr.logical_row_id || $3::text)
                    LIMIT $4
                """
                cursor_params = [commit_id, pattern, str(sample_params['seed']), sample_params['sample_size']]
            else:
                # True random sampling using TABLESAMPLE
                total_rows = await self.count_table_rows(commit_id, table_key)
                sample_pct = min(100, (sample_params['sample_size'] / max(total_rows, 1)) * 100 * 1.5)
                
                query = f"""
                    SELECT r.data, cr.logical_row_id
                    FROM dsa_core.commit_rows cr
                    TABLESAMPLE SYSTEM({sample_pct})
                    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                    WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE $2
                    LIMIT $3
                """
                cursor_params = [commit_id, pattern, sample_params['sample_size']]
        
        elif sample_method == 'systematic':
            # Systematic sampling with interval
            query = """
                WITH numbered_data AS (
                    SELECT 
                        r.data, cr.logical_row_id,
                        ROW_NUMBER() OVER (ORDER BY cr.logical_row_id) as rn
                    FROM dsa_core.commit_rows cr
                    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                    WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE $2
                )
                SELECT data, logical_row_id
                FROM numbered_data
                WHERE MOD(rn + $3 - 1, $4) = 0
            """
            cursor_params = [
                commit_id, pattern,
                sample_params.get('start', 1),
                sample_params['interval']
            ]
        
        else:
            raise ValueError(f"Unsupported sampling method: {sample_method}")
        
        # Stream results using cursor
        async with self._conn.transaction():
            async for row in self._conn.cursor(query, *cursor_params):
                # Parse JSON data if needed
                data = row['data']
                if isinstance(data, str):
                    data = json.loads(data)
                
                # Handle nested data structure
                if 'data' in data and isinstance(data['data'], dict):
                    actual_data = data['data']
                    yield {
                        '_logical_row_id': row['logical_row_id'],
                        **actual_data
                    }
                else:
                    yield {
                        '_logical_row_id': row['logical_row_id'],
                        **data
                    }