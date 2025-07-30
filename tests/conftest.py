"""Pytest configuration and fixtures."""

import asyncio
import pytest
import pytest_asyncio
from httpx import AsyncClient
from typing import AsyncGenerator, Dict, Any
import os
from datetime import datetime

from src.main import app
from src.infrastructure.postgres.database import DatabasePool
from src.api.dependencies import set_database_pool, set_event_bus
from src.core.events.publisher import EventBus


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def db_pool():
    """Create a database pool for tests."""
    # Use test database configuration
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", 5432))
    database = os.getenv("DB_NAME", "dsa_db")
    user = os.getenv("DB_USER", "dsa_user")
    password = os.getenv("DB_PASSWORD", "dsa_password")
    
    # Create DSN
    from urllib.parse import quote_plus
    dsn = f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{database}"
    
    pool = DatabasePool(dsn)
    await pool.initialize(min_size=1, max_size=5)
    
    # Set global pool for app
    set_database_pool(pool)
    
    yield pool
    
    await pool.close()


@pytest_asyncio.fixture
async def client(db_pool) -> AsyncGenerator[AsyncClient, None]:
    """Create an async HTTP client for testing."""
    # Initialize event bus
    event_bus = EventBus()
    set_event_bus(event_bus)
    
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def test_dataset(client: AsyncClient) -> Dict[str, Any]:
    """Create a test dataset."""
    response = await client.post(
        "/api/datasets",
        headers={"Authorization": "Bearer test-token"},
        json={
            "name": f"test_dataset_{datetime.now().timestamp()}",
            "description": "Test dataset for integration tests",
            "tags": ["test", "integration"]
        }
    )
    
    assert response.status_code == 201
    dataset = response.json()
    
    yield dataset
    
    # Cleanup - delete dataset
    await client.delete(
        f"/api/datasets/{dataset['dataset_id']}",
        headers={"Authorization": "Bearer test-token"}
    )


@pytest_asyncio.fixture
async def test_dataset_with_data(client: AsyncClient) -> Dict[str, Any]:
    """Create a test dataset with sample data.
    
    For now, we'll use an existing dataset with data since creating commits
    requires complex setup.
    """
    # List datasets and find one with completed import
    response = await client.get(
        "/api/datasets",
        headers={"Authorization": "Bearer test-token"}
    )
    
    assert response.status_code == 200
    datasets = response.json()["datasets"]
    
    # Find a dataset with completed import
    dataset_with_data = None
    for dataset in datasets:
        if dataset.get("import_status") == "completed":
            dataset_with_data = dataset
            break
    
    if not dataset_with_data:
        pytest.skip("No dataset with completed import found")
    
    # Get the main ref to find commit_id
    response = await client.get(
        f"/api/datasets/{dataset_with_data['dataset_id']}/refs",
        headers={"Authorization": "Bearer test-token"}
    )
    
    assert response.status_code == 200
    refs = response.json()["refs"]
    main_ref = next((r for r in refs if r["ref_name"] == "main"), None)
    
    if not main_ref:
        pytest.skip("No main ref found for dataset")
    
    # Get schema to find table_key
    response = await client.get(
        f"/api/datasets/{dataset_with_data['dataset_id']}/commits/{main_ref['commit_id']}/schema",
        headers={"Authorization": "Bearer test-token"}
    )
    
    table_key = "primary"  # default
    if response.status_code == 200:
        sheets = response.json().get("sheets", [])
        if sheets:
            table_key = sheets[0]["sheet_name"]
    
    return {
        "dataset_id": dataset_with_data["dataset_id"],
        "commit_id": main_ref["commit_id"],
        "table_key": table_key,
        "name": dataset_with_data["name"],
        "description": dataset_with_data["description"]
    }


@pytest_asyncio.fixture
async def auth_headers():
    """Get authentication headers."""
    return {"Authorization": "Bearer test-token"}