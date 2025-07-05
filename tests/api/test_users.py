"""Tests for user API endpoints."""

import pytest
from httpx import AsyncClient


class TestHealthEndpoints:
    """Test health check endpoints."""
    
    async def test_health_check(self, client: AsyncClient):
        """Test health check endpoint."""
        response = await client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data
    
    async def test_root_endpoint(self, client: AsyncClient):
        """Test root endpoint."""
        response = await client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "DSA Platform" in data["message"]


class TestUserAuthentication:
    """Test user authentication endpoints."""
    
    async def test_login_success(self, client: AsyncClient, test_user: dict):
        """Test successful user login."""
        response = await client.post(
            "/api/users/login",
            data={
                "username": test_user["soeid"],
                "password": test_user["password"]
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Check response structure
        assert "access_token" in data
        assert "refresh_token" in data
        assert "token_type" in data
        assert data["token_type"] == "bearer"
        
        # Check user information
        assert data["user_id"] == test_user["id"]
        assert data["soeid"] == test_user["soeid"]
        assert data["role_id"] == test_user["role_id"]
        assert data["role_name"] == "admin"
    
    async def test_login_invalid_password(self, client: AsyncClient, test_user: dict):
        """Test login with invalid password."""
        response = await client.post(
            "/api/users/login",
            data={
                "username": test_user["soeid"],
                "password": "wrongpassword"
            }
        )
        
        assert response.status_code == 401
        assert "Invalid credentials" in response.text
    
    async def test_login_nonexistent_user(self, client: AsyncClient):
        """Test login with non-existent user."""
        response = await client.post(
            "/api/users/login",
            data={
                "username": "NOTEXIST",
                "password": "anypassword"
            }
        )
        
        assert response.status_code == 401
        assert "Invalid credentials" in response.text
    
    async def test_login_missing_fields(self, client: AsyncClient):
        """Test login with missing fields."""
        # Missing password
        response = await client.post(
            "/api/users/login",
            data={"username": "TEST001"}
        )
        assert response.status_code == 422
        
        # Missing username
        response = await client.post(
            "/api/users/login",
            data={"password": "testpass"}
        )
        assert response.status_code == 422
    
    async def test_oauth2_token_endpoint(self, client: AsyncClient, test_user: dict):
        """Test OAuth2 compatible token endpoint."""
        response = await client.post(
            "/api/users/token",
            data={
                "username": test_user["soeid"],
                "password": test_user["password"]
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert "token_type" in data


class TestUserRegistration:
    """Test user registration endpoints."""
    
    async def test_create_user_unauthorized(self, client: AsyncClient):
        """Test creating user without admin privileges."""
        response = await client.post(
            "/api/users/register",
            json={
                "soeid": "NEW0001",
                "password": "newpass123",
                "role_id": 1
            }
        )
        
        assert response.status_code == 403
    
    async def test_create_user_as_admin(self, client: AsyncClient, auth_headers: dict, db_session):
        """Test creating user as admin."""
        # Ensure we have an admin role
        admin_role_id = await db_session.fetchval(
            "SELECT id FROM dsa_auth.roles WHERE role_name = 'admin'"
        )
        
        response = await client.post(
            "/api/users/register",
            json={
                "soeid": "NEW0001",
                "password": "newpass123",
                "role_id": admin_role_id
            },
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["soeid"] == "NEW0001"
        assert data["role_id"] == admin_role_id
        assert "id" in data
        assert "created_at" in data
    
    async def test_create_duplicate_user(self, client: AsyncClient, auth_headers: dict, test_user: dict):
        """Test creating duplicate user."""
        response = await client.post(
            "/api/users/register",
            json={
                "soeid": test_user["soeid"],
                "password": "anypass123",
                "role_id": test_user["role_id"]
            },
            headers=auth_headers
        )
        
        assert response.status_code == 400
        assert "already exists" in response.text
    
    async def test_create_user_invalid_soeid(self, client: AsyncClient, auth_headers: dict):
        """Test creating user with invalid SOEID format."""
        response = await client.post(
            "/api/users/register",
            json={
                "soeid": "SHORT",  # Too short (must be 7 chars)
                "password": "pass123",
                "role_id": 1
            },
            headers=auth_headers
        )
        
        assert response.status_code == 422
        errors = response.json()["detail"]
        assert any("String should have at least 7 characters" in str(error) for error in errors)


class TestTokenValidation:
    """Test JWT token validation."""
    
    async def test_access_protected_endpoint_without_token(self, client: AsyncClient):
        """Test accessing protected endpoint without token."""
        response = await client.post(
            "/api/datasets/",
            json={"name": "test", "description": "test"}
        )
        
        assert response.status_code == 401
        assert "Not authenticated" in response.text
    
    async def test_access_protected_endpoint_with_invalid_token(self, client: AsyncClient):
        """Test accessing protected endpoint with invalid token."""
        headers = {"Authorization": "Bearer invalid-token-here"}
        response = await client.post(
            "/api/datasets/",
            json={"name": "test", "description": "test"},
            headers=headers
        )
        
        assert response.status_code == 401
        assert "Could not validate credentials" in response.text
    
    async def test_access_protected_endpoint_with_expired_token(self, client: AsyncClient):
        """Test accessing protected endpoint with expired token."""
        # This would require creating a token with past expiration
        # For now, we'll skip this test in a real implementation
        pass