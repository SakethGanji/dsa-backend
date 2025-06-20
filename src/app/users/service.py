import bcrypt
from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from app.users.repository import list_users as repo_list_users, create_user as repo_create_user, get_user_by_soeid
from app.users.models import UserCreate, UserOut, DatasetPermissionType, FilePermissionType, DatasetPermission, FilePermission, PermissionGrant
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
        except IntegrityError as e:
            # Check if it's a foreign key violation or unique constraint violation
            error_msg = str(e.orig) if hasattr(e, 'orig') else str(e)
            if 'foreign key' in error_msg.lower():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid role_id. Role does not exist.",
                )
            else:
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
    
    # Dataset permission methods
    async def grant_dataset_permission(
        self,
        dataset_id: int,
        user_id: int,
        permission_type: DatasetPermissionType
    ) -> DatasetPermission:
        """Grant a permission to a user for a dataset"""
        query = sa.text("""
        INSERT INTO dataset_permissions (dataset_id, user_id, permission_type)
        VALUES (:dataset_id, :user_id, :permission_type)
        ON CONFLICT (dataset_id, user_id) DO UPDATE
        SET permission_type = :permission_type
        RETURNING dataset_id, user_id, permission_type;
        """)
        
        values = {
            "dataset_id": dataset_id,
            "user_id": user_id,
            "permission_type": permission_type.value
        }
        
        result = await self.session.execute(query, values)
        await self.session.commit()
        row = result.mappings().first()
        
        return DatasetPermission(**dict(row))
    
    # File permission methods
    async def grant_file_permission(
        self,
        file_id: int,
        user_id: int,
        permission_type: FilePermissionType
    ) -> FilePermission:
        """Grant a permission to a user for a file"""
        query = sa.text("""
        INSERT INTO file_permissions (file_id, user_id, permission_type)
        VALUES (:file_id, :user_id, :permission_type)
        ON CONFLICT (file_id, user_id) DO UPDATE
        SET permission_type = :permission_type
        RETURNING file_id, user_id, permission_type;
        """)
        
        values = {
            "file_id": file_id,
            "user_id": user_id,
            "permission_type": permission_type.value
        }
        
        result = await self.session.execute(query, values)
        await self.session.commit()
        row = result.mappings().first()
        
        return FilePermission(**dict(row))
    
    async def revoke_dataset_permission(
        self,
        dataset_id: int,
        user_id: int,
        permission_type: DatasetPermissionType
    ) -> bool:
        """Revoke a permission from a user for a dataset"""
        query = sa.text("""
        DELETE FROM dataset_permissions
        WHERE dataset_id = :dataset_id
        AND user_id = :user_id
        AND permission_type = :permission_type
        RETURNING dataset_id;
        """)
        
        values = {
            "dataset_id": dataset_id,
            "user_id": user_id,
            "permission_type": permission_type.value
        }
        
        result = await self.session.execute(query, values)
        await self.session.commit()
        
        return result.scalar_one_or_none() is not None
    
    async def revoke_file_permission(
        self,
        file_id: int,
        user_id: int,
        permission_type: FilePermissionType
    ) -> bool:
        """Revoke a permission from a user for a file"""
        query = sa.text("""
        DELETE FROM file_permissions
        WHERE file_id = :file_id
        AND user_id = :user_id
        AND permission_type = :permission_type
        RETURNING file_id;
        """)
        
        values = {
            "file_id": file_id,
            "user_id": user_id,
            "permission_type": permission_type.value
        }
        
        result = await self.session.execute(query, values)
        await self.session.commit()
        
        return result.scalar_one_or_none() is not None
    
    async def check_dataset_permission(
        self,
        dataset_id: int,
        user_id: int,
        permission_type: DatasetPermissionType
    ) -> bool:
        """Check if a user has a specific permission for a dataset"""
        # Admin permission includes all other permissions
        query = sa.text("""
        SELECT COUNT(*) > 0 as has_permission
        FROM dataset_permissions
        WHERE dataset_id = :dataset_id
        AND user_id = :user_id
        AND (permission_type = :permission_type OR permission_type = 'admin');
        """)
        
        values = {
            "dataset_id": dataset_id,
            "user_id": user_id,
            "permission_type": permission_type.value
        }
        
        result = await self.session.execute(query, values)
        return result.scalar_one()
    
    async def check_file_permission(
        self,
        file_id: int,
        user_id: int,
        permission_type: FilePermissionType
    ) -> bool:
        """Check if a user has a specific permission for a file"""
        # Admin permission includes all other permissions
        query = sa.text("""
        SELECT COUNT(*) > 0 as has_permission
        FROM file_permissions
        WHERE file_id = :file_id
        AND user_id = :user_id
        AND (permission_type = :permission_type OR permission_type = 'admin');
        """)
        
        values = {
            "file_id": file_id,
            "user_id": user_id,
            "permission_type": permission_type.value
        }
        
        result = await self.session.execute(query, values)
        return result.scalar_one()
    
    async def list_user_dataset_permissions(
        self,
        user_id: int
    ) -> List[DatasetPermission]:
        """List all dataset permissions for a user"""
        query = """
        SELECT dataset_id, user_id, permission_type
        FROM dataset_permissions
        WHERE user_id = :user_id
        ORDER BY dataset_id, permission_type;"""
        
        values = {"user_id": user_id}
        
        result = await self.session.execute(sa.text(query), values)
        
        return [DatasetPermission(**dict(row)) for row in result.mappings()]
    
    async def list_user_file_permissions(
        self,
        user_id: int
    ) -> List[FilePermission]:
        """List all file permissions for a user"""
        query = sa.text("""
        SELECT file_id, user_id, permission_type
        FROM file_permissions
        WHERE user_id = :user_id
        ORDER BY file_id, permission_type;
        """)
        
        values = {"user_id": user_id}
        
        result = await self.session.execute(query, values)
        
        return [FilePermission(**dict(row)) for row in result.mappings()]
    
    async def list_dataset_permissions(
        self,
        dataset_id: int
    ) -> List[DatasetPermission]:
        """List all permissions for a dataset"""
        query = sa.text("""
        SELECT dataset_id, user_id, permission_type
        FROM dataset_permissions
        WHERE dataset_id = :dataset_id
        ORDER BY user_id, permission_type;
        """)
        
        values = {"dataset_id": dataset_id}
        
        result = await self.session.execute(query, values)
        
        return [DatasetPermission(**dict(row)) for row in result.mappings()]
    
    async def list_file_permissions(
        self,
        file_id: int
    ) -> List[FilePermission]:
        """List all permissions for a file"""
        query = sa.text("""
        SELECT file_id, user_id, permission_type
        FROM file_permissions
        WHERE file_id = :file_id
        ORDER BY user_id, permission_type;
        """)
        
        values = {"file_id": file_id}
        
        result = await self.session.execute(query, values)
        
        return [FilePermission(**dict(row)) for row in result.mappings()]
