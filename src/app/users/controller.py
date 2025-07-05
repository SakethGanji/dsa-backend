"""User controller for HTTP request handling - HOLLOWED OUT FOR BACKEND RESET"""
from sqlalchemy.ext.asyncio import AsyncSession
from app.users.service import UserService
from app.users.models import UserCreate, UserOut

async def list_users_ctrl(session: AsyncSession) -> list[UserOut]:
    """
    List all users in the system.
    
    Implementation Notes:
    1. Create UserService with session
    2. Call list_users service method
    3. Return list of UserOut models
    4. Used by GET /users endpoint
    
    HTTP Response:
    200 OK:
    [
        {
            "id": 1,
            "soeid": "user123",
            "role_id": 2,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z"
        }
    ]
    
    Error Responses:
    - 500: Database error
    
    Args:
        session: Database session from dependency injection
        
    Returns:
        list[UserOut]: List of user models
    """
    raise NotImplementedError("Implement list users controller")

async def create_user_ctrl(payload: UserCreate, session: AsyncSession) -> UserOut:
    """
    Create a new user.
    
    Implementation Notes:
    1. Validate UserCreate payload
    2. Call UserService.create_user
    3. Return created user data
    4. Used by POST /users endpoint
    
    Request Body:
    {
        "soeid": "user123",
        "password": "plain_text_password",
        "role_id": 2
    }
    
    HTTP Response:
    201 Created:
    {
        "id": 1,
        "soeid": "user123",
        "role_id": 2,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    }
    
    Error Responses:
    - 400: Invalid role_id or SOEID exists
    - 422: Validation error
    - 500: Database error
    
    Args:
        payload: UserCreate model with user data
        session: Database session
        
    Returns:
        UserOut: Created user model
    """
    raise NotImplementedError("Implement create user controller")

async def register_user_ctrl(payload: UserCreate, session: AsyncSession) -> UserOut:
    """
    Register a new user (public endpoint).
    
    Implementation Notes:
    1. Same as create_user but for public registration
    2. May have different validation rules
    3. Could default to basic user role
    4. Used by POST /register endpoint
    
    Business Logic:
    - Auto-assign default role (e.g., 'user')
    - Send welcome email (future)
    - Create initial permissions
    
    Request Body:
    {
        "soeid": "user123",
        "password": "plain_text_password",
        "role_id": 2  // May be optional/defaulted
    }
    
    HTTP Response:
    201 Created:
    {
        "id": 1,
        "soeid": "user123",
        "role_id": 2,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    }
    
    Error Responses:
    - 400: SOEID already exists
    - 422: Validation error
    - 500: Database error
    
    Args:
        payload: UserCreate model with registration data
        session: Database session
        
    Returns:
        UserOut: Registered user model
    """
    raise NotImplementedError("Implement register user controller")
