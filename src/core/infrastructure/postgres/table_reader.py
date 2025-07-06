"""PostgreSQL implementation of ITableReader."""

from typing import List, Dict, Any, Optional, AsyncGenerator
import json
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