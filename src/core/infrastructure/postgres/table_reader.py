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
        query = """
            SELECT DISTINCT SPLIT_PART(logical_row_id, ':', 1) AS table_key
            FROM dsa_core.commit_rows
            WHERE commit_id = $1
            ORDER BY table_key
        """
        rows = await self._conn.fetch(query, commit_id)
        return [row['table_key'] for row in rows]
    
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
        pattern = f"{table_key}:%"
        
        if limit is None:
            # Get all rows (use with caution for large datasets)
            query = """
                SELECT r.data, cr.logical_row_id
                FROM dsa_core.commit_rows cr
                JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE $2
                ORDER BY cr.logical_row_id
                OFFSET $3
            """
            rows = await self._conn.fetch(query, commit_id, pattern, offset)
        else:
            # Get paginated rows
            query = """
                SELECT r.data, cr.logical_row_id
                FROM dsa_core.commit_rows cr
                JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE $2
                ORDER BY cr.logical_row_id
                OFFSET $3 LIMIT $4
            """
            rows = await self._conn.fetch(query, commit_id, pattern, offset, limit)
        
        # Parse data and include row index
        result = []
        for row in rows:
            # Extract row index from logical_row_id
            _, row_idx = row['logical_row_id'].split(':', 1)
            
            # Parse JSON data if needed
            data = row['data']
            if isinstance(data, str):
                data = json.loads(data)
            
            # Add row index to data
            result.append({
                '_row_index': int(row_idx),
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
        pattern = f"{table_key}:%"
        query = """
            SELECT COUNT(*)
            FROM dsa_core.commit_rows
            WHERE commit_id = $1 AND logical_row_id LIKE $2
        """
        count = await self._conn.fetchval(query, commit_id, pattern)
        return count or 0