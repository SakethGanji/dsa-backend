"""Fixed test configuration with proper async handling."""

import asyncio
import os
from typing import AsyncGenerator, Generator
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from asyncpg import Connection

# Set test environment variables
os.environ["DATABASE_URL"] = "postgresql://postgres:postgres@localhost:5432/postgres"
os.environ["SECRET_KEY"] = "test-secret-key-for-testing-only"

from src.main import app
from src.core.database import DatabasePool
from src.core.config import get_settings


# Global database pool for tests
test_db_pool = None


# Set pytest-asyncio to use module scope for event loops
pytest_plugins = ('pytest_asyncio',)


@pytest.fixture(scope="session")
def event_loop_policy():
    """Create an event loop policy."""
    return asyncio.get_event_loop_policy()


@pytest.fixture(scope="session")
def event_loop(event_loop_policy) -> Generator:
    """Create an event loop for the test session."""
    loop = event_loop_policy.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
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
    
    def override_get_db_pool():
        return test_db_pool
    
    app.dependency_overrides[main_get_db_pool] = override_get_db_pool
    app.dependency_overrides[users_get_db_pool] = override_get_db_pool
    app.dependency_overrides[datasets_get_db_pool] = override_get_db_pool
    app.dependency_overrides[auth_get_db_pool] = override_get_db_pool
    
    yield
    
    await test_db_pool.close()


@pytest_asyncio.fixture
async def client(setup_database) -> AsyncGenerator[AsyncClient, None]:
    """Create test client."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def db_session(setup_database) -> AsyncGenerator[Connection, None]:
    """Get database connection."""
    async with test_db_pool.acquire() as conn:
        # Start a transaction for test isolation
        tx = conn.transaction()
        await tx.start()
        try:
            yield conn
        finally:
            # Rollback to maintain test isolation
            await tx.rollback()


@pytest_asyncio.fixture
async def test_user(db_session: Connection):
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
    
    return {
        "id": result["id"],
        "soeid": result["soeid"],
        "password": "testpass123",
        "role_id": result["role_id"]
    }


@pytest_asyncio.fixture
async def auth_headers(client: AsyncClient, test_user: dict) -> dict:
    """Get authentication headers for test user."""
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
        # Fallback for tests that don't have full auth setup
        return {"Authorization": "Bearer test-token"}


@pytest_asyncio.fixture
async def test_dataset(db_session: Connection, test_user: dict) -> dict:
    """Create a test dataset."""
    import time
    
    dataset_name = f"test_dataset_{int(time.time())}"
    
    # Create dataset (datasets are in dsa_core schema)
    result = await db_session.fetchrow("""
        INSERT INTO dsa_core.datasets (name, description, created_by)
        VALUES ($1, $2, $3)
        RETURNING id as dataset_id, name, description, created_by
    """, dataset_name, "Test dataset for unit tests", test_user["id"])
    
    # Grant admin permission to test user
    await db_session.execute("""
        INSERT INTO dsa_auth.dataset_permissions (dataset_id, user_id, permission_type)
        VALUES ($1, $2, 'admin'::dsa_auth.dataset_permission)
    """, result["dataset_id"], test_user["id"])
    
    return {
        "id": result["dataset_id"],
        "name": result["name"],
        "description": result["description"],
        "created_by": result["created_by"]
    }


@pytest.fixture
def anyio_backend():
    """Configure anyio backend."""
    return "asyncio"


# Marker configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "api: marks tests as API endpoint tests"
    )