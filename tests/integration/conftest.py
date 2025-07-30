"""Shared fixtures and configuration for integration tests."""

import pytest
import pytest_asyncio
import httpx
import os
import time
import uuid
from typing import AsyncGenerator, Dict, Any, Tuple

# Load config from environment variables with defaults
BASE_URL = os.getenv("TEST_BASE_URL", "http://localhost:8000")
AUTH_TOKEN = os.getenv("TEST_AUTH_TOKEN", "test-token")


@pytest.fixture(scope="session")
def auth_headers() -> Dict[str, str]:
    """Fixture for authentication headers."""
    return {"Authorization": f"Bearer {AUTH_TOKEN}"}


@pytest_asyncio.fixture(scope="function")
async def async_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Fixture for a httpx.AsyncClient instance per test function."""
    async with httpx.AsyncClient(base_url=BASE_URL, follow_redirects=True, timeout=30.0) as client:
        yield client


@pytest_asyncio.fixture(scope="function")
async def created_dataset(async_client: httpx.AsyncClient, auth_headers: Dict[str, str]) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Fixture to create a new dataset for a test and clean it up afterwards.
    Scope is 'function' so each test gets its own dataset.
    """
    dataset_id = None
    # Use time with microseconds to avoid duplicates
    test_dataset_name = f"pytest_dataset_{int(time.time() * 1000000)}"
    
    # SETUP: Create the dataset
    response = await async_client.post(
        "/api/datasets",
        headers=auth_headers,
        json={
            "name": test_dataset_name,
            "description": "Pytest integration test dataset",
            "tags": ["pytest", "automated", "test"]
        }
    )
    assert response.status_code in [200, 201], f"Failed to create dataset: {response.text}"
    dataset_data = response.json()
    dataset_id = dataset_data["dataset_id"]
    
    # Yield the created data to the test function
    yield dataset_data

    # TEARDOWN: Clean up the dataset after the test is done
    if dataset_id:
        try:
            # Note: DELETE endpoint has issues, so we attempt but don't fail if it doesn't work
            delete_response = await async_client.delete(
                f"/api/datasets/{dataset_id}", 
                headers=auth_headers
            )
            if delete_response.status_code not in [200, 204]:
                print(f"\nWARN: Failed to cleanup dataset {dataset_id}. Status: {delete_response.status_code}")
        except Exception as e:
            print(f"\nWARN: Exception during cleanup of dataset {dataset_id}: {e}")


@pytest_asyncio.fixture(scope="function")
async def dataset_with_data(async_client: httpx.AsyncClient, auth_headers: Dict[str, str]) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Fixture that provides a dataset known to have completed import and data.
    For versioning tests that need existing data.
    """
    # First, try to find an existing dataset with completed import
    response = await async_client.get("/api/datasets", headers=auth_headers)
    assert response.status_code == 200
    datasets = response.json()["datasets"]
    
    # Prefer dataset 2 (City_Population_Data) which we know has data
    test_dataset = None
    for ds in datasets:
        if ds["dataset_id"] == 2 and ds.get("import_status") == "completed":
            test_dataset = ds
            break
    
    # Fallback to any dataset with completed import
    if not test_dataset:
        for ds in datasets:
            if ds.get("import_status") == "completed":
                test_dataset = ds
                break
    
    if not test_dataset:
        pytest.skip("No dataset with completed import found")
    
    yield test_dataset
    # No cleanup needed since we're using existing datasets


@pytest.fixture
def test_branch_name() -> str:
    """Generate a unique branch name for testing."""
    return f"test-branch-{int(time.time())}"


@pytest.fixture
def non_existent_dataset_id() -> int:
    """Provide a dataset ID that should not exist."""
    return 99999999


@pytest.fixture
def non_existent_user_soeid() -> str:
    """Provide a user SOEID that should not exist."""
    return "non_existent_test_user"


@pytest_asyncio.fixture(scope="function")
async def dataset_to_be_deleted(async_client: httpx.AsyncClient, auth_headers: Dict[str, str]) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Fixture specifically for delete tests.
    Creates a dataset that is intended to be deleted by the test.
    """
    dataset_name = f"pytest_delete_test_{uuid.uuid4()}"
    
    # SETUP: Create the dataset
    create_response = await async_client.post(
        "/api/datasets",
        headers=auth_headers,
        json={
            "name": dataset_name,
            "description": "Dataset created for deletion testing"
        }
    )
    assert create_response.status_code in [200, 201], f"Failed to create dataset for deletion test: {create_response.text}"
    created_dataset = create_response.json()
    
    yield created_dataset
    
    # TEARDOWN: No cleanup needed since the test should delete it
    # But we'll check if it still exists and clean up if needed
    dataset_id = created_dataset["dataset_id"]
    try:
        check_response = await async_client.get(f"/api/datasets/{dataset_id}", headers=auth_headers)
        if check_response.status_code == 200:
            # Dataset still exists, clean it up
            print(f"\nWARN: Test didn't delete dataset {dataset_id}, cleaning up")
            await async_client.delete(f"/api/datasets/{dataset_id}", headers=auth_headers)
    except Exception:
        pass  # Ignore cleanup errors


@pytest_asyncio.fixture(scope="function")
async def duplicate_dataset_pair(async_client: httpx.AsyncClient, auth_headers: Dict[str, str]) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Fixture for testing duplicate dataset creation.
    Creates the first dataset, returns its info for duplicate testing.
    """
    dataset_name = f"pytest_duplicate_test_{uuid.uuid4()}"
    dataset_id = None
    
    # Create the first dataset
    response = await async_client.post(
        "/api/datasets",
        headers=auth_headers,
        json={
            "name": dataset_name,
            "description": "First dataset for duplicate testing"
        }
    )
    assert response.status_code in [200, 201], f"Failed to create first dataset: {response.text}"
    dataset_data = response.json()
    dataset_id = dataset_data["dataset_id"]
    
    yield dataset_data
    
    # TEARDOWN: Clean up the dataset
    if dataset_id:
        try:
            await async_client.delete(f"/api/datasets/{dataset_id}", headers=auth_headers)
        except Exception as e:
            print(f"\nWARN: Failed to cleanup dataset {dataset_id}: {e}")


@pytest_asyncio.fixture(scope="function")
async def table_info(async_client: httpx.AsyncClient, auth_headers: Dict[str, str], 
                    dataset_with_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Provides table information from a dataset with data.
    Returns commit_id and table_key for use in table-specific tests.
    """
    dataset_id = dataset_with_data["dataset_id"]
    
    # Get refs to find main commit
    refs_response = await async_client.get(f"/api/datasets/{dataset_id}/refs", headers=auth_headers)
    assert refs_response.status_code == 200
    
    refs_data = refs_response.json()
    main_ref = next((r for r in refs_data["refs"] if r["ref_name"] == "main"), None)
    if not main_ref:
        pytest.skip("No main ref found in dataset")
    
    commit_id = main_ref["commit_id"]
    
    # Get schema to find table key
    schema_response = await async_client.get(
        f"/api/datasets/{dataset_id}/commits/{commit_id}/schema",
        headers=auth_headers
    )
    assert schema_response.status_code == 200
    
    schema = schema_response.json()
    if not schema.get("sheets"):
        pytest.skip("Dataset has no tables/sheets to test against")
    
    table_key = schema["sheets"][0]["sheet_name"]
    
    return {
        "dataset_id": dataset_id,
        "commit_id": commit_id,
        "table_key": table_key
    }


@pytest_asyncio.fixture(scope="function")
async def dataset_with_uploaded_file(async_client: httpx.AsyncClient, auth_headers: Dict[str, str]) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Create a dataset by uploading a CSV file and wait for import to complete.
    This ensures we have real commit data with schemas for testing.
    """
    import asyncio
    import os
    
    dataset_name = f"pytest_uploaded_{int(time.time() * 1000000)}"
    dataset_id = None
    
    # Read the test CSV file
    test_file_path = os.path.join(os.path.dirname(__file__), "test_sample.csv")
    
    # Create dataset with file upload
    with open(test_file_path, "rb") as f:
        files = {"file": ("test_sample.csv", f, "text/csv")}
        data = {
            "name": dataset_name,
            "description": "Dataset created from file upload for testing",
            "tags": "pytest,upload,test",
            "default_branch": "main",
            "commit_message": "Initial import from test"
        }
        
        response = await async_client.post(
            "/api/datasets/create-with-file",
            headers=auth_headers,
            files=files,
            data=data
        )
        
    assert response.status_code in [200, 201], f"Failed to create dataset with file: {response.text}"
    
    result = response.json()
    dataset_id = result["dataset"]["dataset_id"]
    import_job_id = result["import_job"]["job_id"]
    
    # Poll until import is completed (max 30 seconds)
    max_wait_time = 30
    poll_interval = 1
    elapsed_time = 0
    
    while elapsed_time < max_wait_time:
        # Get dataset details to check import status
        status_response = await async_client.get(
            f"/api/datasets/{dataset_id}",
            headers=auth_headers
        )
        
        if status_response.status_code == 200:
            dataset_info = status_response.json()
            
            # Handle nested response structure if present
            if "dataset" in dataset_info:
                dataset_info = dataset_info["dataset"]
                
            import_status = dataset_info.get("import_status")
            
            if import_status == "completed":
                # Import completed successfully
                yield {
                    "dataset_id": dataset_id,
                    "name": dataset_name,
                    "import_job_id": import_job_id,
                    "import_status": "completed"
                }
                break
            elif import_status == "failed":
                pytest.fail(f"Dataset import failed for dataset {dataset_id}")
                
        await asyncio.sleep(poll_interval)
        elapsed_time += poll_interval
    else:
        pytest.fail(f"Dataset import timed out after {max_wait_time} seconds")
    
    # Cleanup
    if dataset_id:
        try:
            await async_client.delete(f"/api/datasets/{dataset_id}", headers=auth_headers)
        except Exception as e:
            print(f"\nWARN: Failed to cleanup dataset {dataset_id}: {e}")