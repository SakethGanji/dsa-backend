import bcrypt
from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from app.users.repository import list_users as repo_list_users, create_user as repo_create_user, get_user_by_soeid
from app.users.models import UserCreate, UserOut, Permission, PermissionCreate, PermissionType, ResourceType
from app.users.auth import create_access_token
from typing import List, Dict, Any, Optional
import sqlalchemy as sa

class UserService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def authenticate_user(self, username: str, password: str) -> UserOut | None:
        # TODO: Implement actual authentication logic
        # This is a placeholder implementation
        user = await get_user_by_soeid(self.session, username)
        if not user:
            return None
            
        # Check if password matches
        if bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            # Ensure the user dict has all required fields for UserOut model
            if 'id' not in user or 'soeid' not in user or 'role_id' not in user or 'created_at' not in user or 'updated_at' not in user:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="User data incomplete in database",
                )
            return UserOut.model_validate(user)
        return None

    async def create_user(self, user_create: UserCreate) -> UserOut:
        # Hash the password before storing it
        password_hash = bcrypt.hashpw(user_create.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        try:
            user = await repo_create_user(
                self.session,
                soeid=user_create.soeid,
                password_hash=password_hash,
                role_id=user_create.role_id
            )
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to create user",
                )
            return UserOut.model_validate(user)
        except IntegrityError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User with this SOEID already exists",
            )

    async def list_users(self) -> List[UserOut]:
        # TODO: Implement actual user listing logic
        # This is a placeholder implementation
        users = await repo_list_users(self.session)
        return [UserOut.model_validate(user) for user in users]

    async def get_user_by_soeid(self, soeid: str) -> UserOut | None:
        # TODO: Implement actual logic to get user by SOEID
        # This is a placeholder implementation
        user = await get_user_by_soeid(self.session, soeid)
        if user:
            return UserOut.model_validate(user)
        return None
    
    # Permission methods
    async def grant_permission(
        self,
        resource_type: ResourceType,
        resource_id: int,
        user_id: int,
        permission_type: PermissionType,
        granted_by: int
    ) -> Permission:
        """Grant a permission to a user for a resource"""
        query = sa.text("""
        INSERT INTO permissions (resource_type, resource_id, user_id, permission_type, granted_by)
        VALUES (:resource_type, :resource_id, :user_id, :permission_type, :granted_by)
        ON CONFLICT (resource_type, resource_id, user_id, permission_type) DO UPDATE
        SET granted_at = NOW(), granted_by = :granted_by
        RETURNING id, resource_type, resource_id, user_id, permission_type, granted_at, granted_by;
        """)
        
        values = {
            "resource_type": resource_type.value,
            "resource_id": resource_id,
            "user_id": user_id,
            "permission_type": permission_type.value,
            "granted_by": granted_by
        }
        
        result = await self.session.execute(query, values)
        await self.session.commit()
        row = result.mappings().first()
        
        return Permission(**dict(row))
    
    async def revoke_permission(
        self,
        resource_type: ResourceType,
        resource_id: int,
        user_id: int,
        permission_type: PermissionType
    ) -> bool:
        """Revoke a permission from a user for a resource"""
        query = sa.text("""
        DELETE FROM permissions
        WHERE resource_type = :resource_type
        AND resource_id = :resource_id
        AND user_id = :user_id
        AND permission_type = :permission_type
        RETURNING id;
        """)
        
        values = {
            "resource_type": resource_type.value,
            "resource_id": resource_id,
            "user_id": user_id,
            "permission_type": permission_type.value
        }
        
        result = await self.session.execute(query, values)
        await self.session.commit()
        
        return result.scalar_one_or_none() is not None
    
    async def check_permission(
        self,
        resource_type: ResourceType,
        resource_id: int,
        user_id: int,
        permission_type: PermissionType
    ) -> bool:
        """Check if a user has a specific permission for a resource"""
        # Admin permission includes all other permissions
        query = sa.text("""
        SELECT COUNT(*) > 0 as has_permission
        FROM permissions
        WHERE resource_type = :resource_type
        AND resource_id = :resource_id
        AND user_id = :user_id
        AND (permission_type = :permission_type OR permission_type = 'admin');
        """)
        
        values = {
            "resource_type": resource_type.value,
            "resource_id": resource_id,
            "user_id": user_id,
            "permission_type": permission_type.value
        }
        
        result = await self.session.execute(query, values)
        return result.scalar_one()
    
    async def list_user_permissions(
        self,
        user_id: int,
        resource_type: Optional[ResourceType] = None
    ) -> List[Permission]:
        """List all permissions for a user"""
        query = """
        SELECT id, resource_type, resource_id, user_id, permission_type, granted_at, granted_by
        FROM permissions
        WHERE user_id = :user_id
        """
        
        values = {"user_id": user_id}
        
        if resource_type:
            query += " AND resource_type = :resource_type"
            values["resource_type"] = resource_type.value
        
        query += " ORDER BY resource_type, resource_id, permission_type;"
        
        result = await self.session.execute(sa.text(query), values)
        
        return [Permission(**dict(row)) for row in result.mappings()]
    
    async def list_resource_permissions(
        self,
        resource_type: ResourceType,
        resource_id: int
    ) -> List[Permission]:
        """List all permissions for a resource"""
        query = sa.text("""
        SELECT id, resource_type, resource_id, user_id, permission_type, granted_at, granted_by
        FROM permissions
        WHERE resource_type = :resource_type
        AND resource_id = :resource_id
        ORDER BY user_id, permission_type;
        """)
        
        values = {
            "resource_type": resource_type.value,
            "resource_id": resource_id
        }
        
        result = await self.session.execute(query, values)
        
        return [Permission(**dict(row)) for row in result.mappings()]
