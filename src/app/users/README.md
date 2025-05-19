# Authentication Implementation Guide

## JWT Authentication System

This module implements JWT-based authentication with role-based access control.

## Key Components

1. **Token Creation & Verification**:
   - `create_access_token(subject, role_id)`: Creates a short-lived JWT with user identity and role
   - `create_refresh_token(subject)`: Creates a long-lived token for refreshing access tokens
   - `verify_token(token, token_type)`: Validates a token and extracts user data

2. **User Context**:
   - `CurrentUser`: Data model containing user info from JWT (soeid, role_id)
   - Includes helper methods for permission checks: `is_admin()`, `is_manager()`, etc.

3. **Dependency Injection**:
   - `get_current_user_info()`: FastAPI dependency for protected routes

## Authentication Flow

1. User logs in via `/api/users/token` endpoint with username/password
2. Server issues access_token and refresh_token
3. Client includes access_token in Authorization header for subsequent requests
4. When access_token expires, client uses refresh_token to get a new one

## Using Authentication

### To Protect a Route

```python
@router.get("/secure-endpoint")
async def protected_endpoint(
    current_user: CurrentUser = Depends(get_current_user_info)
):
    # Route is protected - only authenticated users can access
    # Access user info with current_user.soeid, current_user.role_id
    return {"message": f"Hello, {current_user.soeid}!"}
```

### Role-Based Access Control

```python
@router.post("/admin-only")
async def admin_endpoint(
    current_user: CurrentUser = Depends(get_current_user_info)
):
    # Check if user has admin permissions
    if not current_user.is_admin():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return {"message": "Admin access granted"}
```

## Security Notes

- JWT tokens are signed with SECRET_KEY from app.core.config
- Access tokens expire after the time defined in ACCESS_TOKEN_EXPIRE_MINUTES
- Refresh tokens expire after REFRESH_TOKEN_EXPIRE_DAYS
- Always update SECRET_KEY in production environments