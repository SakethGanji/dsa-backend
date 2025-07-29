"""Simplified integration tests for user endpoints that work with current DB state."""

import pytest
import httpx
from typing import Dict, Any

pytestmark = pytest.mark.asyncio


class TestBasicUserEndpoints:
    """Basic tests for user endpoints."""
    
    async def test_unauthorized_access(self, async_client: httpx.AsyncClient):
        """Test that protected endpoints require authentication."""
        # List users requires auth
        response = await async_client.get("/api/users")
        assert response.status_code in [401, 403]
        
        # Register requires admin auth
        response = await async_client.post("/api/users/register", json={})
        assert response.status_code in [401, 403]
        
        # Update requires admin auth
        response = await async_client.put("/api/users/1", json={})
        assert response.status_code in [401, 403]
        
        # Delete requires admin auth
        response = await async_client.delete("/api/users/1")
        assert response.status_code in [401, 403]
    
    async def test_invalid_login_formats(self, async_client: httpx.AsyncClient):
        """Test login with invalid request formats."""
        # Missing fields
        response = await async_client.post(
            "/api/users/login",
            data={"username": "test"}
        )
        assert response.status_code in [400, 422]
        
        # Empty credentials
        response = await async_client.post(
            "/api/users/login",
            data={"username": "", "password": ""}
        )
        assert response.status_code in [400, 422]
    
    async def test_token_endpoint_formats(self, async_client: httpx.AsyncClient):
        """Test token endpoint with various formats."""
        # Valid format but non-existent user
        response = await async_client.post(
            "/api/users/token",
            data={
                "username": "nonexistentuser",
                "password": "somepassword"
            }
        )
        assert response.status_code in [400, 401, 403]
    
    async def test_public_registration_validation(self, async_client: httpx.AsyncClient):
        """Test public registration with invalid data."""
        # Missing required fields
        response = await async_client.post(
            "/api/users/register-public",
            json={"soeid": "test"}
        )
        assert response.status_code in [400, 422]
        
        # Invalid role_id (out of range)
        response = await async_client.post(
            "/api/users/register-public",
            json={
                "soeid": "test123",
                "password": "TestPass123",
                "role_id": 99
            }
        )
        assert response.status_code in [400, 422]
        
        # Password too short
        response = await async_client.post(
            "/api/users/register-public",
            json={
                "soeid": "test123",
                "password": "short",
                "role_id": 1
            }
        )
        assert response.status_code in [400, 422]
    
    async def test_admin_endpoints_with_auth(self, async_client: httpx.AsyncClient, auth_headers: Dict[str, str]):
        """Test admin endpoints return proper errors for non-existent resources."""
        # Update non-existent user
        response = await async_client.put(
            "/api/users/999999",
            headers=auth_headers,
            json={"soeid": "newsoeid"}
        )
        assert response.status_code == 404
        
        # Delete non-existent user  
        response = await async_client.delete(
            "/api/users/999999",
            headers=auth_headers
        )
        assert response.status_code == 404