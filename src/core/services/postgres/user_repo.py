"""PostgreSQL implementation of IUserRepository."""

from typing import Optional, Dict, Any
from asyncpg import Connection
from ...services.interfaces import IUserRepository


class PostgresUserRepository(IUserRepository):
    """PostgreSQL implementation for user management."""
    
    def __init__(self, connection: Connection):
        self._conn = connection
    
    async def get_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by ID."""
        query = """
            SELECT u.id, u.soeid, u.role_id, r.role_name, 
                   u.created_at, u.updated_at
            FROM dsa_auth.users u
            LEFT JOIN dsa_auth.roles r ON u.role_id = r.id
            WHERE u.id = $1
        """
        row = await self._conn.fetchrow(query, user_id)
        return dict(row) if row else None
    
    async def get_by_soeid(self, soeid: str) -> Optional[Dict[str, Any]]:
        """Get user by SOEID."""
        query = """
            SELECT u.id, u.soeid, u.role_id, r.role_name, 
                   u.created_at, u.updated_at
            FROM dsa_auth.users u
            LEFT JOIN dsa_auth.roles r ON u.role_id = r.id
            WHERE u.soeid = $1
        """
        row = await self._conn.fetchrow(query, soeid)
        return dict(row) if row else None
    
    async def create_user(self, soeid: str, password_hash: str, role_id: int) -> int:
        """Create a new user."""
        query = """
            INSERT INTO dsa_auth.users (soeid, password_hash, role_id)
            VALUES ($1, $2, $3)
            RETURNING id
        """
        result = await self._conn.fetchval(query, soeid, password_hash, role_id)
        return result
    
    async def get_user_with_password(self, soeid: str) -> Optional[Dict[str, Any]]:
        """Get user including password hash for authentication."""
        query = """
            SELECT u.id, u.soeid, u.password_hash, u.role_id, r.role_name, 
                   u.created_at, u.updated_at
            FROM dsa_auth.users u
            LEFT JOIN dsa_auth.roles r ON u.role_id = r.id
            WHERE u.soeid = $1
        """
        row = await self._conn.fetchrow(query, soeid)
        return dict(row) if row else None
    
    async def update_user_password(self, user_id: int, new_password_hash: str) -> None:
        """Update user password."""
        query = """
            UPDATE dsa_auth.users 
            SET password_hash = $2, updated_at = NOW()
            WHERE id = $1
        """
        await self._conn.execute(query, user_id, new_password_hash)