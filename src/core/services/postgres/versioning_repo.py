"""PostgreSQL implementation of ICommitRepository."""

from typing import Optional, Dict, Any, List, Tuple, Set
import json
import hashlib
from asyncpg import Connection
from ...services.interfaces import ICommitRepository


class PostgresCommitRepository(ICommitRepository):
    """PostgreSQL implementation for versioning engine operations."""
    
    def __init__(self, connection: Connection):
        self._conn = connection
    
    async def add_rows_if_not_exist(self, rows: Set[Tuple[str, str]]) -> None:
        """Add (row_hash, row_data_json) pairs to rows table."""
        # Convert set to list for bulk insert
        rows_list = list(rows)
        
        # Use COPY for efficient bulk insert
        await self._conn.copy_records_to_table(
            'dsa_core.rows',
            records=rows_list,
            columns=['row_hash', 'data']
        )
    
    async def create_commit_and_manifest(
        self, 
        dataset_id: int,
        parent_commit_id: Optional[str],
        message: str,
        author_id: int,
        manifest: List[Tuple[str, str]]  # List of (logical_row_id, row_hash)
    ) -> str:
        """Create a new commit with its manifest."""
        # Generate content-addressable commit ID
        commit_content = {
            "dataset_id": dataset_id,
            "parent_commit_id": parent_commit_id,
            "manifest": sorted(manifest),  # Sort for consistency
            "message": message,
            "author_id": author_id
        }
        commit_id = hashlib.sha256(
            json.dumps(commit_content, sort_keys=True).encode()
        ).hexdigest()
        
        # Insert commit
        commit_query = """
            INSERT INTO dsa_core.commits (commit_id, dataset_id, parent_commit_id, message, author_id)
            VALUES ($1, $2, $3, $4, $5)
        """
        await self._conn.execute(
            commit_query, 
            commit_id, 
            dataset_id, 
            parent_commit_id, 
            message, 
            author_id
        )
        
        # Bulk insert manifest using COPY
        manifest_records = [
            (commit_id, logical_row_id, row_hash)
            for logical_row_id, row_hash in manifest
        ]
        
        await self._conn.copy_records_to_table(
            'dsa_core.commit_rows',
            records=manifest_records,
            columns=['commit_id', 'logical_row_id', 'row_hash']
        )
        
        return commit_id
    
    async def update_ref_atomically(
        self, 
        dataset_id: int, 
        ref_name: str, 
        new_commit_id: str, 
        expected_commit_id: str
    ) -> bool:
        """Update ref only if it currently points to expected_commit_id."""
        query = """
            UPDATE dsa_core.refs
            SET commit_id = $3
            WHERE dataset_id = $1 AND name = $2 
                AND (commit_id = $4 OR (commit_id IS NULL AND $4 IS NULL))
        """
        result = await self._conn.execute(
            query, 
            dataset_id, 
            ref_name, 
            new_commit_id, 
            expected_commit_id
        )
        # Check if update was successful (1 row affected)
        return result.split()[-1] == '1'
    
    async def get_current_commit_for_ref(self, dataset_id: int, ref_name: str) -> Optional[str]:
        """Get current commit ID for a ref."""
        query = """
            SELECT commit_id
            FROM dsa_core.refs
            WHERE dataset_id = $1 AND name = $2
        """
        return await self._conn.fetchval(query, dataset_id, ref_name)
    
    async def get_commit_data(
        self, 
        commit_id: str, 
        sheet_name: Optional[str] = None, 
        offset: int = 0, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Retrieve data for a commit, optionally filtered by sheet."""
        if sheet_name:
            # Filter by sheet name prefix
            query = """
                SELECT cr.logical_row_id, r.data
                FROM dsa_core.commit_rows cr
                INNER JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE $2
                ORDER BY cr.logical_row_id
                OFFSET $3 LIMIT $4
            """
            pattern = f"{sheet_name}:%"
            rows = await self._conn.fetch(query, commit_id, pattern, offset, limit)
        else:
            # Get all rows
            query = """
                SELECT cr.logical_row_id, r.data
                FROM dsa_core.commit_rows cr
                INNER JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = $1
                ORDER BY cr.logical_row_id
                OFFSET $2 LIMIT $3
            """
            rows = await self._conn.fetch(query, commit_id, offset, limit)
        
        # Parse JSONB data
        result = []
        for row in rows:
            data = json.loads(row['data']) if isinstance(row['data'], str) else row['data']
            result.append({
                'logical_row_id': row['logical_row_id'],
                'data': data
            })
        
        return result
    
    async def create_commit_schema(self, commit_id: str, schema_definition: Dict[str, Any]) -> None:
        """Store schema for a commit."""
        query = """
            INSERT INTO dsa_core.commit_schemas (commit_id, schema_definition)
            VALUES ($1, $2)
        """
        await self._conn.execute(query, commit_id, json.dumps(schema_definition))
    
    async def get_commit_schema(self, commit_id: str) -> Optional[Dict[str, Any]]:
        """Get schema for a commit."""
        query = """
            SELECT schema_definition
            FROM dsa_core.commit_schemas
            WHERE commit_id = $1
        """
        result = await self._conn.fetchval(query, commit_id)
        return json.loads(result) if result else None
    
    async def get_commit_history(self, dataset_id: int, ref_name: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get commit history for a ref."""
        # Recursive CTE to traverse commit history
        query = """
            WITH RECURSIVE commit_history AS (
                -- Start from the current ref
                SELECT c.* 
                FROM dsa_core.commits c
                INNER JOIN dsa_core.refs r ON c.commit_id = r.commit_id
                WHERE r.dataset_id = $1 AND r.name = $2
                
                UNION ALL
                
                -- Recursively get parent commits
                SELECT c.*
                FROM dsa_core.commits c
                INNER JOIN commit_history ch ON c.commit_id = ch.parent_commit_id
            )
            SELECT commit_id, dataset_id, parent_commit_id, message, 
                   author_id, committed_at
            FROM commit_history
            ORDER BY committed_at DESC
            LIMIT $3
        """
        rows = await self._conn.fetch(query, dataset_id, ref_name, limit)
        return [dict(row) for row in rows]