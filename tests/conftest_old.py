"""Pytest configuration and fixtures for DSA Platform tests."""

import asyncio
import os
from typing import AsyncGenerator, Generator
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine
from fastapi import FastAPI

from src.main import app
from src.core.database import DatabasePool
from src.core.config import get_settings


# Override test settings
os.environ["DATABASE_URL"] = "postgresql://postgres:postgres@localhost:5432/postgres"
os.environ["SECRET_KEY"] = "test-secret-key-for-testing-only"


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an instance of the default event loop for the test session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def db_pool() -> AsyncGenerator[DatabasePool, None]:
    """Create a database pool for testing."""
    settings = get_settings()
    pool = DatabasePool(settings.database_url)
    await pool.initialize()
    yield pool
    await pool.close()


@pytest_asyncio.fixture(scope="function")
async def db_session(db_pool: DatabasePool):
    """Create a database session with transaction rollback."""
    # For now, just use the pool directly without transaction wrapping
    # This simplifies testing while we debug the async issues
    async with db_pool.acquire() as conn:
        yield conn


@pytest_asyncio.fixture(scope="function")
async def test_app(db_pool: DatabasePool) -> FastAPI:
    """Create a test FastAPI application."""
    # Override the database pool dependency
    from src.main import get_db_pool
    
    async def override_get_db_pool():
        return db_pool
    
    app.dependency_overrides[get_db_pool] = override_get_db_pool
    
    return app


@pytest_asyncio.fixture(scope="function")
async def client(test_app: FastAPI) -> AsyncGenerator[AsyncClient, None]:
    """Create an async HTTP client for testing."""
    from httpx import ASGITransport
    
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture(scope="function")
async def test_user(db_session):
    """Create a test user."""
    from passlib.context import CryptContext
    
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    
    # Ensure admin role exists
    await db_session.execute("""
        INSERT INTO dsa_auth.roles (role_name, description) 
        VALUES ('admin', 'Administrator role')
        ON CONFLICT (role_name) DO NOTHING
    """)
    
    # Create test user
    result = await db_session.fetchrow("""
        INSERT INTO dsa_auth.users (soeid, password_hash, role_id)
        SELECT 'TEST999', $1, id FROM dsa_auth.roles WHERE role_name = 'admin'
        RETURNING id, soeid, role_id
    """, pwd_context.hash("testpass123"))
    
    return {
        "id": result["id"],
        "soeid": result["soeid"],
        "password": "testpass123",
        "role_id": result["role_id"]
    }


@pytest_asyncio.fixture(scope="function")
async def auth_headers(client: AsyncClient, test_user: dict) -> dict:
    """Get authorization headers with a valid JWT token."""
    response = await client.post(
        "/api/users/login",
        data={
            "username": test_user["soeid"],
            "password": test_user["password"]
        }
    )
    assert response.status_code == 200
    token = response.json()["access_token"]
    
    return {"Authorization": f"Bearer {token}"}


@pytest_asyncio.fixture(scope="function")
async def test_dataset(db_session, test_user: dict):
    """Create a test dataset."""
    # Create dataset
    dataset_id = await db_session.fetchval("""
        INSERT INTO dsa_core.datasets (name, description, created_by)
        VALUES ('test_dataset', 'Test dataset for unit tests', $1)
        RETURNING id
    """, test_user["id"])
    
    # Create default ref
    await db_session.execute("""
        INSERT INTO dsa_core.refs (dataset_id, name, commit_id)
        VALUES ($1, 'main', NULL)
    """, dataset_id)
    
    # Grant admin permission
    await db_session.execute("""
        INSERT INTO dsa_auth.dataset_permissions (dataset_id, user_id, permission_type)
        VALUES ($1, $2, 'admin'::dsa_auth.dataset_permission)
    """, dataset_id, test_user["id"])
    
    return {
        "id": dataset_id,
        "name": "test_dataset",
        "created_by": test_user["id"]
    }