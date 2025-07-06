"""Simplified test configuration for API tests."""

import os
import pytest
import pytest_asyncio
from httpx import AsyncClient
from typing import AsyncGenerator

# Set test environment variables
os.environ["DATABASE_URL"] = "postgresql://postgres:postgres@localhost:5432/postgres"
os.environ["SECRET_KEY"] = "test-secret-key-for-testing-only"

from src.main import app
from src.core.database import DatabasePool
from src.core.config import get_settings


@pytest_asyncio.fixture(scope="function")
async def client() -> AsyncGenerator[AsyncClient, None]:
    """Create test client."""
    from httpx import ASGITransport
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture(scope="function")
async def db_pool() -> AsyncGenerator[DatabasePool, None]:
    """Create database pool for tests."""
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
    
    yield pool
    
    await pool.close()
    app.dependency_overrides.clear()


@pytest_asyncio.fixture(scope="function") 
async def db_session(db_pool: DatabasePool):
    """Get database connection for tests."""
    async with db_pool.acquire() as conn:
        yield conn


@pytest_asyncio.fixture(scope="function")
async def test_user(db_pool: DatabasePool):
    """Create a test user."""
    from passlib.context import CryptContext
    
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    
    async with db_pool.acquire() as conn:
        # Ensure admin role exists
        await conn.execute("""
            INSERT INTO dsa_auth.roles (role_name, description) 
            VALUES ('admin', 'Administrator role')
            ON CONFLICT (role_name) DO NOTHING
        """)
        
        # Create test user
        result = await conn.fetchrow("""
            INSERT INTO dsa_auth.users (soeid, password_hash, role_id)
            SELECT 'TEST999', $1, id FROM dsa_auth.roles WHERE role_name = 'admin'
            ON CONFLICT (soeid) DO UPDATE SET password_hash = EXCLUDED.password_hash
            RETURNING id, soeid, role_id
        """, pwd_context.hash("testpass123"))
        
        return {
            "id": result["id"],
            "soeid": result["soeid"],
            "password": "testpass123",
            "role_id": result["role_id"]
        }


@pytest_asyncio.fixture(scope="function")
async def auth_headers(client: AsyncClient, test_user: dict, db_pool: DatabasePool) -> dict:
    """Get authentication headers for test user."""
    # Ensure the app uses our test pool
    from src.main import get_db_pool as main_get_db_pool
    app.dependency_overrides[main_get_db_pool] = lambda: db_pool
    
    # Login to get token
    response = await client.post(
        "/api/users/login",
        data={
            "username": test_user["soeid"],
            "password": test_user["password"]
        }
    )
    
    if response.status_code == 200:
        token = response.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}
    else:
        raise Exception(f"Failed to login: {response.status_code} - {response.text}")


@pytest_asyncio.fixture(scope="function")
async def test_dataset(db_pool: DatabasePool, test_user: dict) -> dict:
    """Create a test dataset."""
    import time
    
    dataset_name = f"test_dataset_{int(time.time())}"
    
    async with db_pool.acquire() as conn:
        # Create dataset
        result = await conn.fetchrow("""
            INSERT INTO dsa_core.datasets (name, description, created_by)
            VALUES ($1, $2, $3)
            RETURNING id as dataset_id, name, description, created_by
        """, dataset_name, "Test dataset for unit tests", test_user["id"])
        
        # Grant admin permission to test user
        await conn.execute("""
            INSERT INTO dsa_auth.dataset_permissions (dataset_id, user_id, permission_type)
            VALUES ($1, $2, 'admin'::dsa_auth.dataset_permission)
        """, result["dataset_id"], test_user["id"])
        
        return {
            "id": result["dataset_id"],
            "name": result["name"],
            "description": result["description"],
            "created_by": result["created_by"]
        }