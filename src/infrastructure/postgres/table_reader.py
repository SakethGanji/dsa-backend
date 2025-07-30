"""PostgreSQL implementation of ITableReader."""

from typing import List, Dict, Any, Optional, AsyncGenerator
import json
import re
from asyncpg import Connection
class PostgresTableReader:
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
        # First try to get by table_key directly (for newer schemas)
        query = """
            SELECT schema_definition -> $2 AS table_schema
            FROM dsa_core.commit_schemas
            WHERE commit_id = $1
        """
        result = await self._conn.fetchval(query, commit_id, table_key)
        
        if result:
            # asyncpg may return JSONB as string, parse it if needed
            if isinstance(result, str):
                import json
                return json.loads(result)
            return result
            
        # Fallback: Get full schema and extract the sheet
        query = """
            SELECT schema_definition
            FROM dsa_core.commit_schemas
            WHERE commit_id = $1
        """
        full_schema = await self._conn.fetchval(query, commit_id)
        
        if full_schema and 'sheets' in full_schema:
            # Find the matching sheet
            for sheet in full_schema['sheets']:
                if sheet.get('sheet_name') == table_key:
                    return sheet
                    
        return None
    
    async def get_table_statistics(self, commit_id: str, table_key: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific table within a commit."""
        query = """
            SELECT analysis -> 'statistics' AS table_stats
            FROM dsa_core.table_analysis
            WHERE commit_id = $1 AND table_key = $2
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
        # Handle both formats: "table_key:hash" and "table_key_hash"
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
            
            # All data follows the standardized format:
            # { "sheet_name": "...", "row_number": N, "data": {...} }
            if 'data' not in data or not isinstance(data['data'], dict):
                raise ValueError(f"Invalid data format - expected standardized format with 'data' field")
            
            # Extract the actual data from the standardized structure
            actual_data = data['data']
            
            result.append({
                '_logical_row_id': row['logical_row_id'],
                **actual_data
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
                # Parse JSON data if needed
                data = row['data']
                if isinstance(data, str):
                    data = json.loads(data)
                
                batch.append({
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
        # Handle both formats: "table_key:hash" and "table_key_hash"
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
    
    async def batch_get_table_metadata(self, commit_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Batch fetch table metadata for multiple commits in a single operation."""
        # First, get all schemas for the commits
        schema_query = """
            SELECT 
                cs.commit_id,
                cs.schema_definition,
                cs.created_at
            FROM dsa_core.commit_schemas cs
            WHERE cs.commit_id = ANY($1::text[])
        """
        
        schema_rows = await self._conn.fetch(schema_query, commit_ids)
        
        # Then get row counts for each table
        count_query = """
            SELECT 
                commit_id,
                CASE 
                    WHEN logical_row_id LIKE '%:%' THEN SPLIT_PART(logical_row_id, ':', 1)
                    ELSE REGEXP_REPLACE(logical_row_id, '_[0-9]+$', '')
                END AS table_key,
                COUNT(*) as row_count
            FROM dsa_core.commit_rows
            WHERE commit_id = ANY($1::text[])
            GROUP BY commit_id, table_key
        """
        
        count_rows = await self._conn.fetch(count_query, commit_ids)
        
        # Build a lookup for row counts
        row_counts = {}
        for row in count_rows:
            commit_id = row['commit_id'].strip()
            table_key = row['table_key']
            if commit_id not in row_counts:
                row_counts[commit_id] = {}
            row_counts[commit_id][table_key] = row['row_count']
        
        # Process results
        result = {}
        for schema_row in schema_rows:
            commit_id = schema_row['commit_id'].strip()
            schema_def = schema_row['schema_definition']
            created_at = schema_row['created_at']
            
            if commit_id not in result:
                result[commit_id] = []
            
            # Parse schema to get table information
            if schema_def:
                # Handle different schema formats
                if isinstance(schema_def, str):
                    schema_def = json.loads(schema_def)
                
                # Extract table keys and column info
                tables_to_process = []
                
                # Check for direct table keys (newer format)
                if isinstance(schema_def, dict):
                    for table_key, table_schema in schema_def.items():
                        if isinstance(table_schema, dict) and 'columns' in table_schema:
                            tables_to_process.append({
                                'table_key': table_key,
                                'columns': table_schema['columns']
                            })
                    
                    # Also check for 'sheets' format (Excel files)
                    if 'sheets' in schema_def:
                        for sheet in schema_def['sheets']:
                            if 'sheet_name' in sheet and 'columns' in sheet:
                                tables_to_process.append({
                                    'table_key': sheet['sheet_name'],
                                    'columns': sheet['columns']
                                })
                
                # Create metadata for each table
                for table_data in tables_to_process:
                    table_key = table_data['table_key']
                    columns = table_data['columns']
                    
                    # Get column count
                    column_count = len(columns) if columns else 0
                    
                    # Get row count from our lookup
                    row_count = row_counts.get(commit_id, {}).get(table_key, 0)
                    
                    metadata = {
                        'table_key': table_key,
                        'row_count': row_count,
                        'column_count': column_count,
                        'created_at': created_at
                    }
                    
                    result[commit_id].append(metadata)
        
        return result