"""PostgreSQL implementation of ICommitRepository."""

from typing import Optional, Dict, Any, List, Tuple, Set
import json
import hashlib
from asyncpg import Connection
from ...abstractions.repositories import ICommitRepository


class PostgresCommitRepository(ICommitRepository):
    """PostgreSQL implementation for versioning engine operations."""
    
    def __init__(self, connection: Connection):
        self._conn = connection
    
    async def add_rows_if_not_exist(self, rows: Set[Tuple[str, str]]) -> None:
        """Add (row_hash, row_data_json) pairs to rows table."""
        if not rows:
            return
            
        # Use INSERT with ON CONFLICT to handle existing rows
        query = """
            INSERT INTO dsa_core.rows (row_hash, data)
            VALUES ($1, $2::jsonb)
            ON CONFLICT (row_hash) DO NOTHING
        """
        
        # Convert to list and prepare for batch insert
        rows_list = [(row_hash, data) for row_hash, data in rows]
        
        # Execute batch insert
        await self._conn.executemany(query, rows_list)
    
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
        # Insert manifest records
        manifest_query = """
            INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
            VALUES ($1, $2, $3)
        """
        
        manifest_records = [
            (commit_id, logical_row_id, row_hash)
            for logical_row_id, row_hash in manifest
        ]
        
        await self._conn.executemany(manifest_query, manifest_records)
        
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
    
    async def get_ref(self, dataset_id: int, ref_name: str) -> Optional[Dict[str, Any]]:
        """Get ref details including commit_id."""
        query = """
            SELECT name, commit_id
            FROM dsa_core.refs
            WHERE dataset_id = $1 AND name = $2
        """
        row = await self._conn.fetchrow(query, dataset_id, ref_name)
        return dict(row) if row else None
    
    async def get_commit_data(
        self, 
        commit_id: str, 
        table_key: Optional[str] = None, 
        offset: int = 0, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Retrieve data for a commit, optionally filtered by table.
        
        DEPRECATED: Use ITableReader.get_table_data() instead for consistent table-aware data access.
        This method will be removed in a future version.
        """
        if table_key:
            # Filter by table key prefix
            query = """
                SELECT cr.logical_row_id, r.data
                FROM dsa_core.commit_rows cr
                INNER JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE $2
                ORDER BY cr.logical_row_id
                OFFSET $3 LIMIT $4
            """
            pattern = f"{table_key}:%"
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
    
    async def get_commit_history(self, dataset_id: int, ref_name: str = "main", offset: int = 0, limit: int = 50) -> List[Dict[str, Any]]:
        """Get commit history for a specific ref with pagination."""
        # Recursive CTE to traverse commit history from specified ref
        query = """
            WITH RECURSIVE commit_history AS (
                -- Start from the specified ref
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
            SELECT 
                commit_id, 
                dataset_id, 
                parent_commit_id, 
                message, 
                author_id, 
                committed_at as created_at,
                (SELECT COUNT(*) FROM dsa_core.commit_rows WHERE commit_id = commit_history.commit_id) as row_count
            FROM commit_history
            ORDER BY committed_at DESC
            LIMIT $3 OFFSET $4
        """
        rows = await self._conn.fetch(query, dataset_id, ref_name, limit, offset)
        return [dict(row) for row in rows]
    
    async def create_commit_statistics(self, commit_id: str, statistics: Dict[str, Any]) -> None:
        """Store statistics for a commit."""
        query = """
            INSERT INTO dsa_core.commit_statistics (commit_id, statistics)
            VALUES ($1, $2)
        """
        await self._conn.execute(query, commit_id, json.dumps(statistics))
    
    async def create_commit_schema(self, commit_id: str, schema_definition: Dict[str, Any]) -> None:
        """Store schema for a commit."""
        query = """
            INSERT INTO dsa_core.commit_schemas (commit_id, schema_definition)
            VALUES ($1, $2)
            ON CONFLICT (commit_id) DO UPDATE SET schema_definition = $2
        """
        await self._conn.execute(query, commit_id, json.dumps(schema_definition))
    
    async def get_commit_by_id(self, commit_id: str) -> Optional[Dict[str, Any]]:
        """Get commit details including author info."""
        query = """
            SELECT commit_id, dataset_id, parent_commit_id, message, 
                   author_id, committed_at as created_at
            FROM dsa_core.commits 
            WHERE commit_id = $1
        """
        row = await self._conn.fetchrow(query, commit_id)
        return dict(row) if row else None
    
    async def count_commits_for_dataset(self, dataset_id: int, ref_name: str = "main") -> int:
        """Count total commits for a dataset starting from a specific ref."""
        query = """
            WITH RECURSIVE commit_history AS (
                -- Start from the specified ref
                SELECT c.commit_id, c.parent_commit_id
                FROM dsa_core.commits c
                INNER JOIN dsa_core.refs r ON c.commit_id = r.commit_id
                WHERE r.dataset_id = $1 AND r.name = $2
                
                UNION ALL
                
                -- Recursively get parent commits
                SELECT c.commit_id, c.parent_commit_id
                FROM dsa_core.commits c
                INNER JOIN commit_history ch ON c.parent_commit_id = ch.commit_id
            )
            SELECT COUNT(*) FROM commit_history
        """
        result = await self._conn.fetchval(query, dataset_id, ref_name)
        return result or 0
    
    async def count_commit_rows(self, commit_id: str, table_key: Optional[str] = None) -> int:
        """Count rows in a commit, optionally filtered by table."""
        if table_key:
            query = """
                SELECT COUNT(*) FROM dsa_core.commit_rows 
                WHERE commit_id = $1 AND logical_row_id LIKE $2
            """
            result = await self._conn.fetchval(query, commit_id, f"{table_key}:%")
        else:
            query = """
                SELECT COUNT(*) FROM dsa_core.commit_rows WHERE commit_id = $1
            """
            result = await self._conn.fetchval(query, commit_id)
        
        return result or 0
    
    async def list_refs(self, dataset_id: int) -> List[Dict[str, Any]]:
        """List all refs/branches for a dataset."""
        query = """
            SELECT r.id, r.name, r.commit_id, r.dataset_id,
                   c.committed_at as created_at,
                   c.committed_at as updated_at
            FROM dsa_core.refs r
            JOIN dsa_core.commits c ON r.commit_id = c.commit_id
            WHERE r.dataset_id = $1
            ORDER BY r.name
        """
        rows = await self._conn.fetch(query, dataset_id)
        return [dict(row) for row in rows]
    
    async def create_ref(self, dataset_id: int, ref_name: str, commit_id: str) -> None:
        """Create a new ref pointing to a specific commit."""
        query = """
            INSERT INTO dsa_core.refs (dataset_id, name, commit_id)
            VALUES ($1, $2, $3)
        """
        try:
            await self._conn.execute(query, dataset_id, ref_name, commit_id)
        except Exception as e:
            if "unique" in str(e).lower():
                raise ValueError(f"Ref '{ref_name}' already exists for dataset {dataset_id}")
            raise
    
    async def delete_ref(self, dataset_id: int, ref_name: str) -> bool:
        """Delete a ref. Returns True if deleted, False if not found."""
        query = """
            DELETE FROM dsa_core.refs
            WHERE dataset_id = $1 AND name = $2
        """
        result = await self._conn.execute(query, dataset_id, ref_name)
        # PostgreSQL returns "DELETE n" where n is rows affected
        return result.split()[-1] != "0"
    
    async def get_default_branch(self, dataset_id: int) -> Optional[str]:
        """Get the default branch name for a dataset (usually 'main')."""
        # For now, we'll hardcode 'main' as default
        # In future, this could be stored in datasets table
        return "main"