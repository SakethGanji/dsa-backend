"""Simplified test configuration."""

import asyncio
import os
from typing import AsyncGenerator
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

# Set test environment variables
os.environ["DATABASE_URL"] = "postgresql://postgres:postgres@localhost:5432/postgres"
os.environ["SECRET_KEY"] = "test-secret-key-for-testing-only"

from src.main import app
from src.core.database import DatabasePool
from src.core.config import get_settings


# Global database pool for tests
test_db_pool = None


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    """Create event loop for entire test session."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_database():
    """Setup database pool for all tests."""
    global test_db_pool
    settings = get_settings()
    test_db_pool = DatabasePool(settings.database_url)
    await test_db_pool.initialize()
    
    # Override the dependencies in the app
    from src.main import get_db_pool as main_get_db_pool
    from src.api.users import get_db_pool as users_get_db_pool
    from src.api.datasets import get_db_pool as datasets_get_db_pool
    from src.core.authorization import get_db_pool as auth_get_db_pool
    
    app.dependency_overrides[main_get_db_pool] = lambda: test_db_pool
    app.dependency_overrides[users_get_db_pool] = lambda: test_db_pool
    app.dependency_overrides[datasets_get_db_pool] = lambda: test_db_pool
    app.dependency_overrides[auth_get_db_pool] = lambda: test_db_pool
    
    yield
    
    await test_db_pool.close()


@pytest_asyncio.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    """Create test client."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def db_session():
    """Get database connection."""
    async with test_db_pool.acquire() as conn:
        yield conn


@pytest_asyncio.fixture
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
        ON CONFLICT (soeid) DO UPDATE SET password_hash = EXCLUDED.password_hash
        RETURNING id, soeid, role_id
    """, pwd_context.hash("testpass123"))
    
    yield {
        "id": result["id"],
        "soeid": result["soeid"],
        "password": "testpass123",
        "role_id": result["role_id"]
    }
    
    # Cleanup
    await db_session.execute("DELETE FROM dsa_auth.users WHERE soeid = 'TEST999'")


@pytest_asyncio.fixture
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