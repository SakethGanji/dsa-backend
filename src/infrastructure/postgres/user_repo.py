"""PostgreSQL implementation of IUserRepository."""

from typing import Optional, Dict, Any, List
from asyncpg import Connection
# Remove interface import
from .base_repository import BasePostgresRepository


class PostgresUserRepository(BasePostgresRepository[int]):
    """PostgreSQL implementation for user management."""
    
    def __init__(self, connection: Connection):
        super().__init__(
            connection=connection,
            table_name="dsa_auth.users",
            id_column="id",
            id_type=int
        )
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
    
    async def update_user(self, user_id: int, soeid: Optional[str] = None, role_id: Optional[int] = None) -> bool:
        """Update user information."""
        updates = {}
        if soeid is not None:
            updates["soeid"] = soeid
        if role_id is not None:
            updates["role_id"] = role_id
        
        if not updates:
            return True  # Nothing to update
        
        updates["updated_at"] = "NOW()"  # This will be handled specially in the query
        
        # Build dynamic update query
        set_clauses = []
        values = []
        param_count = 1
        
        for field, value in updates.items():
            if field == "updated_at":
                set_clauses.append(f"{field} = {value}")
            else:
                set_clauses.append(f"{field} = ${param_count}")
                values.append(value)
                param_count += 1
        
        # Add user_id as last parameter
        values.append(user_id)
        
        query = f"""
            UPDATE dsa_auth.users
            SET {', '.join(set_clauses)}
            WHERE id = ${param_count}
            RETURNING id
        """
        
        result = await self._conn.fetchval(query, *values)
        return result is not None
    
    async def delete_user(self, user_id: int) -> bool:
        """Delete a user - uses base class implementation."""
        return await self.delete(user_id)
    
    async def count_users_by_role(self, role_name: str) -> int:
        """Count users with a specific role."""
        query = """
            SELECT COUNT(*)
            FROM dsa_auth.users u
            JOIN dsa_auth.roles r ON u.role_id = r.id
            WHERE r.role_name = $1
        """
        return await self._conn.fetchval(query, role_name)
    
    async def list_users(self, offset: int = 0, limit: int = 100, role_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """List users with pagination and optional role filter."""
        query = """
            SELECT u.id, u.soeid, u.role_id, r.role_name, 
                   u.created_at, u.updated_at
            FROM dsa_auth.users u
            LEFT JOIN dsa_auth.roles r ON u.role_id = r.id
        """
        
        values = []
        param_count = 0
        
        if role_filter:
            param_count += 1
            query += f" WHERE r.role_name = ${param_count}"
            values.append(role_filter)
        
        query += " ORDER BY u.id"
        
        # Add pagination
        param_count += 1
        offset_param = param_count
        param_count += 1
        limit_param = param_count
        
        query += f" OFFSET ${offset_param} LIMIT ${limit_param}"
        values.extend([offset, limit])
        
        rows = await self._conn.fetch(query, *values)
        return [dict(row) for row in rows]