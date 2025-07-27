# Users Service Consolidation Plan

## Current State Analysis

Currently, the users functionality is split across 6 separate handler files:
1. `CreateUserHandler` - Creates new users (admin only)
2. `CreateUserPublicHandler` - Creates users via public endpoint (testing only)
3. `LoginUserHandler` - Authenticates users and generates JWT tokens
4. `ListUsersHandler` - Lists all users with pagination (admin only)
5. `UpdateUserHandler` - Updates user information (admin only)
6. `DeleteUserHandler` - Deletes users (admin only)

### Key Observations:
- More complex than other features due to authentication and authorization concerns
- Uses external dependencies: `PasswordHasher`, `PostgresUserRepository`
- Implements domain models (`User`, `UserRole`, `UserCredentials`)
- Includes event publishing for audit trail
- Mix of public (login) and admin-only operations
- Some handlers use base handler patterns (`BaseHandler`, `BaseUpdateHandler`)
- Authentication logic is separate from CRUD operations

## Proposed Solution: Consolidated UserService

### Benefits:
1. **Unified user management** - Single service for all user operations
2. **Better security boundaries** - Clear separation of public vs admin methods
3. **Centralized password handling** - Single point for password hashing
4. **Easier testing** - Mock one service instead of 6 handlers
5. **Consistent patterns** - Matches other consolidated services

### Proposed Structure:

```python
# src/features/users/services/user_service.py

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.user_repo import PostgresUserRepository
from src.infrastructure.external.password_hasher import PasswordHasher
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus
from src.core.domain_exceptions import (
    ConflictException, 
    EntityNotFoundException, 
    ValidationException,
    BusinessRuleViolation
)
from src.core.auth import create_access_token, create_refresh_token
from ...base_handler import with_transaction, with_error_handling
from ..models import (
    CreateUserCommand,
    UpdateUserCommand,
    DeleteUserCommand,
    ListUsersCommand,
    User,
    UserRole,
    UserCredentials
)


@dataclass
class LoginResponse:
    """Response for user login."""
    access_token: str
    refresh_token: str
    token_type: str
    user_id: int
    soeid: str
    role_id: int
    role_name: str


@dataclass
class UserResponse:
    """Response for user operations."""
    id: int
    soeid: str
    role_id: int
    role_name: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_login: Optional[datetime] = None


@dataclass
class UserListItem:
    """Single item in user list."""
    id: int
    soeid: str
    role_id: int
    role_name: Optional[str]
    created_at: datetime
    last_login: Optional[datetime]
    dataset_count: Optional[int] = 0


class UserService:
    """Consolidated service for all user operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        user_repo: PostgresUserRepository,
        permissions: PermissionService,
        password_hasher: Optional[PasswordHasher] = None,
        event_bus: Optional[EventBus] = None
    ):
        self._uow = uow
        self._user_repo = user_repo
        self._permissions = permissions
        self._password_hasher = password_hasher or PasswordHasher()
        self._event_bus = event_bus
    
    # ========== Public Methods (No Auth Required) ==========
    
    @with_error_handling
    async def login(
        self,
        soeid: str,
        password: str
    ) -> LoginResponse:
        """Authenticate user and generate tokens."""
        # Implementation from LoginUserHandler
        pass
    
    # ========== Admin-Only Methods ==========
    
    @with_transaction
    @with_error_handling
    async def create_user(
        self,
        command: CreateUserCommand
    ) -> UserResponse:
        """Create a new user (admin only)."""
        # Check permissions
        await self._permissions.require_role(command.created_by, "admin")
        
        # Implementation from CreateUserHandler
        pass
    
    @with_error_handling
    async def list_users(
        self,
        command: ListUsersCommand
    ) -> Tuple[List[UserListItem], int]:
        """List all users with pagination (admin only)."""
        # Check permissions
        await self._permissions.require_role(command.user_id, "admin")
        
        # Implementation from ListUsersHandler
        pass
    
    @with_transaction
    @with_error_handling
    async def update_user(
        self,
        command: UpdateUserCommand
    ) -> UserResponse:
        """Update user information (admin only)."""
        # Check permissions
        await self._permissions.require_role(command.user_id, "admin")
        
        # Implementation from UpdateUserHandler
        pass
    
    @with_transaction
    @with_error_handling
    async def delete_user(
        self,
        command: DeleteUserCommand
    ) -> Dict[str, Any]:
        """Delete a user (admin only)."""
        # Check permissions
        await self._permissions.require_role(command.user_id, "admin")
        
        # Implementation from DeleteUserHandler
        pass
    
    # ========== Test-Only Methods ==========
    
    @with_transaction
    @with_error_handling
    async def create_user_public(
        self,
        soeid: str,
        password: str,
        role_id: Optional[int] = None
    ) -> UserResponse:
        """Create user without authentication (TEST ONLY)."""
        # Implementation from CreateUserPublicHandler
        # Should be disabled in production
        pass
    
    # ========== Private Helper Methods ==========
    
    async def _validate_unique_soeid(self, soeid: str, exclude_user_id: Optional[int] = None):
        """Validate SOEID is unique."""
        existing_user = await self._user_repo.get_by_soeid(soeid)
        if existing_user and existing_user['id'] != exclude_user_id:
            raise ConflictException(
                f"User with SOEID {soeid} already exists",
                entity_type="User",
                entity_id=soeid
            )
    
    async def _ensure_admin_exists(self):
        """Ensure at least one admin user exists."""
        admin_count = await self._user_repo.count_users_by_role('admin')
        if admin_count <= 1:
            raise BusinessRuleViolation(
                "Cannot delete the last admin user", 
                rule="maintain_admin_count"
            )
```

## Implementation Steps

### 1. Create Service Structure
```bash
mkdir -p src/features/users/services
touch src/features/users/services/__init__.py
touch src/features/users/services/user_service.py
```

### 2. Migrate Handler Logic
- Copy each handler's logic into corresponding service methods
- Consolidate common validation logic (e.g., SOEID uniqueness)
- Maintain event publishing for audit trail
- Keep domain model usage (User, UserRole, UserCredentials)

### 3. Update API Endpoints
Transform each endpoint to use the service:
```python
# From:
handler = CreateUserHandler(uow, user_repo, permissions)
return await handler.handle(command)

# To:
service = UserService(uow, user_repo, permissions)
return await service.create_user(command)
```

### 4. Special Considerations

#### Authentication Endpoints
- Login endpoint doesn't require authentication
- Keep OAuth2 compatibility for /token endpoint
- Consider creating a separate AuthService in the future

#### Dependency Injection
- UserRepository is passed separately (not part of UoW)
- PasswordHasher is optional with default
- EventBus is optional for event publishing

#### Test Endpoint
- Keep create_user_public for testing
- Add clear warnings about production usage
- Consider environment-based enabling

### 5. Clean Up
- Remove old handler files
- Update imports throughout codebase
- Update module exports

## Migration Checklist

- [ ] Create services directory structure
- [ ] Create UserService class with structure
- [ ] Migrate login logic (public method)
- [ ] Migrate create_user logic (admin method)
- [ ] Migrate list_users logic (admin method)
- [ ] Migrate update_user logic (admin method)
- [ ] Migrate delete_user logic (admin method)
- [ ] Migrate create_user_public logic (test method)
- [ ] Add shared validation helpers
- [ ] Update API endpoints in `src/api/users.py`
- [ ] Update authentication endpoints
- [ ] Remove old handler files
- [ ] Update handler exports
- [ ] Test all endpoints
- [ ] Verify JWT token generation

## Common Pitfalls to Avoid

1. **Repository Pattern**: UserRepository is not part of UoW - handle separately
2. **Password Security**: Always hash passwords before storage
3. **Permission Checks**: Ensure all admin methods check permissions
4. **Event Publishing**: Maintain audit trail through events
5. **Domain Models**: Keep using domain models for business logic
6. **Self-Operations**: Prevent users from deleting themselves

## Expected Outcome

After consolidation:
- Single `UserService` class with clear method organization
- Separation of public, admin, and test methods
- Shared validation logic reduces duplication
- Better testability with single service
- Consistent with other service patterns
- Maintains all security features

## Testing Commands

After implementation:
```bash
# Check Python syntax
python3 -m py_compile src/api/users.py src/features/users/services/user_service.py

# Run server
python3 -m uvicorn src.main:app --reload

# Test endpoints
# Login
curl -X POST http://localhost:8000/users/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin123"

# Create user (requires auth token)
curl -X POST http://localhost:8000/users/register \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"soeid": "newuser", "password": "pass123", "role_id": 1}'
```

## Future Enhancements

1. **Separate Auth Service**: Extract authentication logic
2. **Refresh Token Handling**: Implement token refresh endpoint
3. **Password Reset**: Add forgot password functionality
4. **User Profiles**: Extended user information
5. **Session Management**: Track active sessions
6. **Two-Factor Auth**: Enhanced security options