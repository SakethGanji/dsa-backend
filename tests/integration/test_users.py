"""Integration tests for user management endpoints using pytest."""

import pytest
import httpx
import time
import uuid
from typing import Dict, Any

# Mark all tests in this module as async
pytestmark = pytest.mark.asyncio


class TestUserRegistration:
    """Tests for user registration endpoints."""
    
    async def test_register_user_success(self, async_client: httpx.AsyncClient, auth_headers: Dict[str, str]):
        """Test creating a new user with valid data (admin only)."""
        # SOEID must be max 20 chars and alphanumeric only
        test_soeid = f"pt{int(time.time() % 1000000000)}"
        
        response = await async_client.post(
            "/api/users/register",
            headers=auth_headers,
            json={
                "soeid": test_soeid,
                "password": "SecurePassword123!",
                "role_id": 2,  # Regular user role
                "is_active": True
            }
        )
        assert response.status_code in [200, 201]
        
        created_user = response.json()
        assert "user_id" in created_user
        assert created_user.get("soeid") == test_soeid
        assert created_user.get("role_id") == 2
    
    async def test_register_public_user_success(self, async_client: httpx.AsyncClient):
        """Test creating a new user via public registration endpoint."""
        test_soeid = f"pub{int(time.time() % 1000000000)}"
        
        response = await async_client.post(
            "/api/users/register-public",
            json={
                "soeid": test_soeid,
                "password": "SecurePassword123!",
                "role_id": 2  # Regular user role
            }
        )
        assert response.status_code in [200, 201]
        
        created_user = response.json()
        assert "user_id" in created_user
        assert created_user.get("soeid") == test_soeid
    
    async def test_register_duplicate_soeid(self, async_client: httpx.AsyncClient):
        """Test registering a user with duplicate SOEID."""
        test_soeid = f"dup{int(time.time() % 1000000000)}"
        
        # Create first user via public endpoint
        response1 = await async_client.post(
            "/api/users/register-public",
            json={
                "soeid": test_soeid,
                "password": "SecurePassword123!",
                "role_id": 2
            }
        )
        assert response1.status_code in [200, 201]
        
        # Try to create second user with same SOEID
        response2 = await async_client.post(
            "/api/users/register-public",
            json={
                "soeid": test_soeid,
                "password": "DifferentPassword123!",
                "role_id": 2
            }
        )
        assert response2.status_code in [400, 409, 422]
    
    async def test_register_invalid_role_id(self, async_client: httpx.AsyncClient):
        """Test registering a user with invalid role_id."""
        test_soeid = f"inv{int(time.time() % 1000000000)}"
        
        response = await async_client.post(
            "/api/users/register-public",
            json={
                "soeid": test_soeid,
                "password": "SecurePassword123!",
                "role_id": 99  # Invalid role ID
            }
        )
        assert response.status_code in [400, 422]
    
    async def test_register_missing_required_fields(self, async_client: httpx.AsyncClient):
        """Test registering a user with missing required fields."""
        response = await async_client.post(
            "/api/users/register-public",
            json={
                "soeid": "test_user"
                # Missing password and role_id
            }
        )
        assert response.status_code in [400, 422]


class TestUserAuthentication:
    """Tests for user authentication endpoints."""
    
    async def test_login_success(self, async_client: httpx.AsyncClient):
        """Test logging in with valid credentials."""
        # First create a user via public endpoint
        test_soeid = f"log{int(time.time() % 1000000000)}"
        test_password = "SecurePassword123!"
        
        register_response = await async_client.post(
            "/api/users/register-public",
            json={
                "soeid": test_soeid,
                "password": test_password,
                "role_id": 2
            }
        )
        assert register_response.status_code in [200, 201]
        
        # Now try to login using OAuth2 form data
        login_response = await async_client.post(
            "/api/users/login",
            data={  # Form data for OAuth2PasswordRequestForm
                "username": test_soeid,  # username field contains SOEID
                "password": test_password
            }
        )
        assert login_response.status_code == 200
        
        login_data = login_response.json()
        assert "access_token" in login_data
        assert "token_type" in login_data
    
    async def test_login_invalid_credentials(self, async_client: httpx.AsyncClient):
        """Test logging in with invalid credentials."""
        response = await async_client.post(
            "/api/users/login",
            data={  # Form data for OAuth2
                "username": "nonexistent_user",
                "password": "wrongpassword"
            }
        )
        assert response.status_code in [401, 403]
    
    async def test_token_endpoint(self, async_client: httpx.AsyncClient):
        """Test token generation endpoint."""
        # First create a user
        test_soeid = f"tok{int(time.time() % 1000000000)}"
        test_password = "SecurePassword123!"
        
        # Register user via public endpoint
        register_response = await async_client.post(
            "/api/users/register-public",
            json={
                "soeid": test_soeid,
                "password": test_password,
                "role_id": 2
            }
        )
        assert register_response.status_code in [200, 201]
        
        # Test token endpoint
        token_response = await async_client.post(
            "/api/users/token",
            data={  # Form data for OAuth2
                "username": test_soeid,  # username field contains SOEID
                "password": test_password
            }
        )
        assert token_response.status_code == 200
        
        token_data = token_response.json()
        assert "access_token" in token_data
        assert "token_type" in token_data
    
    async def test_login_missing_fields(self, async_client: httpx.AsyncClient):
        """Test login with missing fields."""
        response = await async_client.post(
            "/api/users/login",
            data={  # Form data
                "username": "test_user"
                # Missing password
            }
        )
        assert response.status_code in [400, 422]


class TestUserManagement:
    """Tests for user CRUD operations."""
    
    async def test_list_users(self, async_client: httpx.AsyncClient, auth_headers: Dict[str, str]):
        """Test listing users with authentication."""
        response = await async_client.get("/api/users", headers=auth_headers)
        assert response.status_code == 200
        
        data = response.json()
        assert "users" in data
        assert "total" in data
        assert isinstance(data["users"], list)
    
    async def test_list_users_unauthorized(self, async_client: httpx.AsyncClient):
        """Test listing users without authentication."""
        response = await async_client.get("/api/users")
        assert response.status_code in [401, 403]
    
    async def test_update_user_success(self, async_client: httpx.AsyncClient, auth_headers: Dict[str, str]):
        """Test updating user information."""
        # First create a user via admin endpoint
        test_soeid = f"upd{int(time.time() % 1000000000)}"
        
        create_response = await async_client.post(
            "/api/users/register",
            headers=auth_headers,
            json={
                "soeid": test_soeid,
                "password": "SecurePassword123!",
                "role_id": 2
            }
        )
        assert create_response.status_code in [200, 201]
        
        created_user = create_response.json()
        user_id = created_user["user_id"]
        
        # Update the user
        update_response = await async_client.put(
            f"/api/users/{user_id}",
            headers=auth_headers,
            json={
                "soeid": f"u{test_soeid}",
                "role_id": 3  # Change role
            }
        )
        assert update_response.status_code in [200, 204]
        
        if update_response.status_code == 200:
            updated_user = update_response.json()
            assert updated_user.get("soeid") == f"updated_{test_soeid}"
            assert updated_user.get("role_id") == 3
    
    async def test_update_nonexistent_user(self, async_client: httpx.AsyncClient, auth_headers: Dict[str, str]):
        """Test updating a non-existent user."""
        non_existent_id = 999999
        
        response = await async_client.put(
            f"/api/users/{non_existent_id}",
            headers=auth_headers,
            json={
                "soeid": "new_soeid"
            }
        )
        assert response.status_code == 404
    
    async def test_delete_user_success(self, async_client: httpx.AsyncClient, auth_headers: Dict[str, str]):
        """Test deleting a user."""
        # First create a user to delete via admin endpoint
        test_soeid = f"del{int(time.time() % 1000000000)}"
        
        create_response = await async_client.post(
            "/api/users/register",
            headers=auth_headers,
            json={
                "soeid": test_soeid,
                "password": "SecurePassword123!",
                "role_id": 2
            }
        )
        assert create_response.status_code in [200, 201]
        
        created_user = create_response.json()
        user_id = created_user["user_id"]
        
        # Delete the user
        delete_response = await async_client.delete(
            f"/api/users/{user_id}",
            headers=auth_headers
        )
        assert delete_response.status_code in [200, 204]
        
        # Note: We can't verify deletion by GET since there's no GET /users/{id} endpoint
    
    async def test_delete_nonexistent_user(self, async_client: httpx.AsyncClient, auth_headers: Dict[str, str]):
        """Test deleting a non-existent user."""
        non_existent_id = 999999
        
        response = await async_client.delete(
            f"/api/users/{non_existent_id}",
            headers=auth_headers
        )
        assert response.status_code == 404
    
    async def test_update_user_unauthorized(self, async_client: httpx.AsyncClient):
        """Test updating user without authentication."""
        response = await async_client.put(
            "/api/users/1",
            json={
                "soeid": "new_soeid"
            }
        )
        assert response.status_code in [401, 403]
    
    async def test_delete_user_unauthorized(self, async_client: httpx.AsyncClient):
        """Test deleting user without authentication."""
        response = await async_client.delete("/api/users/1")
        assert response.status_code in [401, 403]