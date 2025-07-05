"""PostgreSQL implementation of IDatasetRepository."""

from typing import Optional, Dict, Any, List
from asyncpg import Connection
from ...services.interfaces import IDatasetRepository


class PostgresDatasetRepository(IDatasetRepository):
    """PostgreSQL implementation for dataset and permission management."""
    
    def __init__(self, connection: Connection):
        self._conn = connection
    
    async def create_dataset(self, name: str, description: str, created_by: int) -> int:
        """Create a new dataset."""
        query = """
            INSERT INTO dsa_core.datasets (name, description, created_by)
            VALUES ($1, $2, $3)
            RETURNING id
        """
        dataset_id = await self._conn.fetchval(query, name, description, created_by)
        
        # Create default 'main' ref for the dataset with NULL commit
        # The commit_id will be set when the first commit is created
        ref_query = """
            INSERT INTO dsa_core.refs (dataset_id, name, commit_id)
            VALUES ($1, 'main', NULL)
            ON CONFLICT (dataset_id, name) DO NOTHING
        """
        await self._conn.execute(ref_query, dataset_id)
        
        # Grant owner permission to creator
        perm_query = """
            INSERT INTO dsa_auth.dataset_permissions (dataset_id, user_id, permission_type)
            VALUES ($1, $2, 'admin')
        """
        await self._conn.execute(perm_query, dataset_id, created_by)
        
        return dataset_id
    
    async def get_dataset_by_id(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """Get dataset by ID."""
        query = """
            SELECT id as dataset_id, name, description, created_by, created_at, updated_at
            FROM dsa_core.datasets
            WHERE id = $1
        """
        row = await self._conn.fetchrow(query, dataset_id)
        return dict(row) if row else None
    
    async def check_user_permission(self, dataset_id: int, user_id: int, required_permission: str) -> bool:
        """Check if user has required permission on dataset."""
        # Permission hierarchy: admin > write > read
        permission_hierarchy = {
            'read': ['read', 'write', 'admin'],
            'write': ['write', 'admin'],
            'admin': ['admin']
        }
        
        allowed_permissions = permission_hierarchy.get(required_permission, [])
        if not allowed_permissions:
            return False
        
        query = """
            SELECT EXISTS(
                SELECT 1 FROM dsa_auth.dataset_permissions
                WHERE dataset_id = $1 AND user_id = $2 AND permission_type = ANY($3::dsa_auth.dataset_permission[])
            )
        """
        return await self._conn.fetchval(query, dataset_id, user_id, allowed_permissions)
    
    async def grant_permission(self, dataset_id: int, user_id: int, permission_type: str) -> None:
        """Grant permission to user on dataset."""
        query = """
            INSERT INTO dsa_auth.dataset_permissions (dataset_id, user_id, permission_type)
            VALUES ($1, $2, $3::dsa_auth.dataset_permission)
            ON CONFLICT (dataset_id, user_id) 
            DO UPDATE SET permission_type = $3::dsa_auth.dataset_permission
        """
        await self._conn.execute(query, dataset_id, user_id, permission_type)
    
    async def list_user_datasets(self, user_id: int) -> List[Dict[str, Any]]:
        """List all datasets accessible to a user."""
        query = """
            SELECT DISTINCT d.id as dataset_id, d.name, d.description, d.created_by, 
                   d.created_at, d.updated_at, p.permission_type::text
            FROM dsa_core.datasets d
            INNER JOIN dsa_auth.dataset_permissions p ON d.id = p.dataset_id
            WHERE p.user_id = $1
            ORDER BY d.created_at DESC
        """
        rows = await self._conn.fetch(query, user_id)
        return [dict(row) for row in rows]