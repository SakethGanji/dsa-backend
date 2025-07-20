"""Handler for creating users (public endpoint - for testing only)."""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from src.core.abstractions import IUnitOfWork
from src.core.abstractions.external import IPasswordManager
from src.core.domain_exceptions import ConflictException
from src.infrastructure.postgres.database import DatabasePool
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.external.password_hasher import PasswordHasher


@dataclass
class CreateUserPublicCommand:
    """Command to create a user via public endpoint."""
    soeid: str
    password: str
    role_id: Optional[int] = None


@dataclass
class CreateUserPublicResponse:
    """Response for user creation."""
    user_id: int
    soeid: str
    role_id: int
    role_name: Optional[str]
    is_active: bool
    created_at: datetime


class CreateUserPublicHandler:
    """Handler for creating users via public endpoint (testing only)."""
    
    def __init__(self, pool: DatabasePool, password_manager: IPasswordManager = None):
        self._pool = pool
        self._password_manager = password_manager or PasswordHasher()
    
    async def handle(self, command: CreateUserPublicCommand) -> CreateUserPublicResponse:
        """Create a new user."""
        uow = PostgresUnitOfWork(self._pool)
        async with uow:
            # Check if user already exists
            existing_user = await uow.users.get_by_soeid(command.soeid)
            if existing_user:
                raise ConflictException(
                    f"User with SOEID {command.soeid} already exists",
                    conflicting_field="soeid",
                    existing_value=command.soeid
                )
            
            # Hash the password
            password_hash = self._password_manager.hash_password(command.password)
            
            # Ensure role exists (default to admin role for testing)
            role_id = command.role_id
            if not role_id:
                # Get or create admin role
                role = await uow.connection.fetchrow("""
                    INSERT INTO dsa_auth.roles (role_name, description) 
                    VALUES ('admin', 'Administrator role')
                    ON CONFLICT (role_name) DO UPDATE SET role_name = EXCLUDED.role_name
                    RETURNING id
                """)
                role_id = role['id']
            
            # Create user
            user_id = await uow.users.create_user(
                soeid=command.soeid,
                password_hash=password_hash,
                role_id=role_id
            )
            
            # Get the created user details
            user = await uow.users.get_by_id(user_id)
            
            return CreateUserPublicResponse(
                user_id=user['id'],
                soeid=user['soeid'],
                role_id=user['role_id'],
                role_name=user.get('role_name'),
                is_active=user.get('is_active', True),
                created_at=user['created_at']
            )