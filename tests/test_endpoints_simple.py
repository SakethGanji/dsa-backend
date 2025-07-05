"""Simple endpoint tests without complex fixtures."""

import asyncio
import os
from httpx import AsyncClient, ASGITransport
import pytest

# Set environment
os.environ["DATABASE_URL"] = "postgresql://postgres:postgres@localhost:5432/postgres"
os.environ["SECRET_KEY"] = "test-secret-key"

from src.main import app
from src.core.database import DatabasePool
from src.core.config import get_settings


@pytest.mark.asyncio
async def test_health_endpoint():
    """Test health endpoint."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data


@pytest.mark.asyncio
async def test_user_login():
    """Test user login flow."""
    # Setup database
    settings = get_settings()
    pool = DatabasePool(settings.database_url)
    await pool.initialize()
    
    # Override dependencies
    from src.main import get_db_pool as main_get_db_pool
    from src.api.users import get_db_pool as users_get_db_pool
    from src.api.datasets import get_db_pool as datasets_get_db_pool
    from src.core.authorization import get_db_pool as auth_get_db_pool
    
    app.dependency_overrides[main_get_db_pool] = lambda: pool
    app.dependency_overrides[users_get_db_pool] = lambda: pool
    app.dependency_overrides[datasets_get_db_pool] = lambda: pool
    app.dependency_overrides[auth_get_db_pool] = lambda: pool
    
    try:
        # Create test user
        async with pool.acquire() as conn:
            # Ensure role exists
            await conn.execute("""
                INSERT INTO dsa_auth.roles (role_name, description) 
                VALUES ('admin', 'Administrator role')
                ON CONFLICT (role_name) DO NOTHING
            """)
            
            # Create user
            from passlib.context import CryptContext
            pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
            
            user = await conn.fetchrow("""
                INSERT INTO dsa_auth.users (soeid, password_hash, role_id)
                SELECT 'TESTUSER', $1, id FROM dsa_auth.roles WHERE role_name = 'admin'
                ON CONFLICT (soeid) DO UPDATE SET password_hash = EXCLUDED.password_hash
                RETURNING id, soeid
            """, pwd_context.hash("testpass"))
        
        # Test login
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/users/login",
                data={
                    "username": "TESTUSER",
                    "password": "testpass"
                }
            )
            assert response.status_code == 200
            data = response.json()
            assert "access_token" in data
            assert "refresh_token" in data
            
        # Cleanup
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM dsa_auth.users WHERE soeid = 'TESTUSER'")
            
    finally:
        await pool.close()
        app.dependency_overrides.clear()


@pytest.mark.asyncio 
async def test_dataset_creation():
    """Test dataset creation with auth."""
    # Setup database
    settings = get_settings()
    pool = DatabasePool(settings.database_url)
    await pool.initialize()
    
    # Override dependencies
    from src.main import get_db_pool as main_get_db_pool
    from src.api.users import get_db_pool as users_get_db_pool
    from src.api.datasets import get_db_pool as datasets_get_db_pool
    from src.core.authorization import get_db_pool as auth_get_db_pool
    
    app.dependency_overrides[main_get_db_pool] = lambda: pool
    app.dependency_overrides[users_get_db_pool] = lambda: pool
    app.dependency_overrides[datasets_get_db_pool] = lambda: pool
    app.dependency_overrides[auth_get_db_pool] = lambda: pool
    
    try:
        # Create test user and get token
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO dsa_auth.roles (role_name, description) 
                VALUES ('admin', 'Administrator role')
                ON CONFLICT (role_name) DO NOTHING
            """)
            
            from passlib.context import CryptContext
            pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
            
            user = await conn.fetchrow("""
                INSERT INTO dsa_auth.users (soeid, password_hash, role_id)
                SELECT 'TESTUSER2', $1, id FROM dsa_auth.roles WHERE role_name = 'admin'
                ON CONFLICT (soeid) DO UPDATE SET password_hash = EXCLUDED.password_hash
                RETURNING id, soeid
            """, pwd_context.hash("testpass"))
            user_id = user["id"]
        
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Login
            response = await client.post(
                "/api/users/login",
                data={
                    "username": "TESTUSER2", 
                    "password": "testpass"
                }
            )
            token = response.json()["access_token"]
            headers = {"Authorization": f"Bearer {token}"}
            
            # Create dataset
            response = await client.post(
                "/api/datasets/",
                json={
                    "name": "test_dataset_123",
                    "description": "Test dataset"
                },
                headers=headers
            )
            if response.status_code != 200:
                print(f"Error response: {response.json()}")
            assert response.status_code == 200
            data = response.json()
            assert "dataset_id" in data
            dataset_id = data["dataset_id"]
            
        # Cleanup
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM dsa_core.datasets WHERE id = $1", dataset_id)
            await conn.execute("DELETE FROM dsa_auth.users WHERE soeid = 'TESTUSER2'")
            
    finally:
        await pool.close()
        app.dependency_overrides.clear()