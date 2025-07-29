"""Integration tests for job service functionality."""

import pytest
import pytest_asyncio
import httpx
import asyncio
import time
from typing import Dict, Any
from pathlib import Path
import tempfile
import csv


@pytest.mark.asyncio
async def test_create_dataset_with_file_and_poll_job(
    async_client: httpx.AsyncClient,
    auth_headers: Dict[str, str]
):
    """Test creating a dataset with file upload and polling the import job."""
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'age', 'city', 'salary'])
        writer.writerow([1, 'John Doe', 30, 'New York', 75000])
        writer.writerow([2, 'Jane Smith', 25, 'Los Angeles', 65000])
        writer.writerow([3, 'Bob Johnson', 35, 'Chicago', 85000])
        writer.writerow([4, 'Alice Williams', 28, 'Houston', 70000])
        writer.writerow([5, 'Charlie Brown', 32, 'Phoenix', 80000])
        temp_file_path = f.name
    
    try:
        # 1. Create dataset with file upload
        with open(temp_file_path, 'rb') as file:
            files = {"file": ("test_data.csv", file, "text/csv")}
            data = {
                "name": f"Test Job Service {int(time.time() * 1000000)}",
                "description": "Testing job service functionality",
                "tags": "pytest,jobs,test"
            }
            
            response = await async_client.post(
                "/api/datasets/create-with-file",
                headers=auth_headers,
                files=files,
                data=data
            )
        
        assert response.status_code == 200, f"Failed to create dataset: {response.text}"
        result = response.json()
        
        assert "dataset" in result
        assert "import_job" in result
        
        dataset_id = result["dataset"]["dataset_id"]
        job_id = result["import_job"]["job_id"]
        
        # 2. Poll job status until completion
        max_attempts = 30
        attempt = 0
        job_status = None
        
        while attempt < max_attempts:
            response = await async_client.get(
                f"/api/jobs/{job_id}",
                headers=auth_headers
            )
            
            assert response.status_code == 200, f"Failed to get job status: {response.text}"
            job_status = response.json()
            
            status = job_status.get("status")
            
            if status == "completed":
                break
            elif status == "failed":
                pytest.fail(f"Job failed: {job_status.get('error_message')}")
            
            await asyncio.sleep(1)
            attempt += 1
        
        assert job_status is not None
        assert job_status["status"] == "completed", f"Job did not complete successfully: {job_status}"
        assert job_status.get("output_summary") is not None
        assert "rows_imported" in job_status["output_summary"]
        assert job_status["output_summary"]["rows_imported"] == 5
        
        # Cleanup
        try:
            await async_client.delete(
                f"/api/datasets/{dataset_id}",
                headers=auth_headers
            )
        except Exception:
            pass  # Ignore cleanup errors
            
    finally:
        # Remove temporary file
        Path(temp_file_path).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_get_jobs_list(
    async_client: httpx.AsyncClient,
    auth_headers: Dict[str, str],
    dataset_with_uploaded_file: Dict[str, Any]
):
    """Test getting list of jobs with various filters."""
    dataset_id = dataset_with_uploaded_file["dataset_id"]
    
    # Test getting all jobs for the dataset
    response = await async_client.get(
        "/api/jobs",
        headers=auth_headers,
        params={"dataset_id": dataset_id}
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert "jobs" in data
    assert "total" in data
    assert "offset" in data
    assert "limit" in data
    
    assert data["total"] >= 1  # At least the import job
    assert len(data["jobs"]) >= 1
    
    # Verify job structure
    job = data["jobs"][0]
    # Check for either 'id' or 'job_id' (API might use either)
    assert "id" in job or "job_id" in job
    assert "run_type" in job
    assert "status" in job
    assert "created_at" in job
    assert "user_id" in job
    
    # Test filtering by run_type
    response = await async_client.get(
        "/api/jobs",
        headers=auth_headers,
        params={
            "dataset_id": dataset_id,
            "run_type": "import"
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    
    # All returned jobs should be import type
    for job in data["jobs"]:
        assert job["run_type"] == "import"
    
    # Test pagination
    response = await async_client.get(
        "/api/jobs",
        headers=auth_headers,
        params={
            "dataset_id": dataset_id,
            "limit": 5,
            "offset": 0
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["limit"] == 5
    assert data["offset"] == 0


@pytest.mark.asyncio
async def test_get_job_by_id_detailed(
    async_client: httpx.AsyncClient,
    auth_headers: Dict[str, str],
    dataset_with_uploaded_file: Dict[str, Any]
):
    """Test getting detailed job information by ID."""
    import_job_id = dataset_with_uploaded_file["import_job_id"]
    
    # Get job details
    response = await async_client.get(
        f"/api/jobs/{import_job_id}",
        headers=auth_headers
    )
    
    assert response.status_code == 200
    job = response.json()
    
    # Verify detailed job information
    # Check for either 'id' or 'job_id' field
    job_id_field = job.get("id") or job.get("job_id")
    assert job_id_field == import_job_id
    assert job["run_type"] == "import"
    assert job["status"] in ["pending", "running", "completed", "failed", "cancelled"]
    assert job["dataset_id"] == dataset_with_uploaded_file["dataset_id"]
    assert "created_at" in job
    assert "user_id" in job
    assert "user_soeid" in job
    
    # If completed, should have additional fields
    if job["status"] == "completed":
        assert "completed_at" in job
        assert "output_summary" in job


@pytest.mark.asyncio
async def test_get_nonexistent_job(
    async_client: httpx.AsyncClient,
    auth_headers: Dict[str, str]
):
    """Test getting a non-existent job returns 404."""
    fake_job_id = "00000000-0000-0000-0000-000000000000"
    
    response = await async_client.get(
        f"/api/jobs/{fake_job_id}",
        headers=auth_headers
    )
    
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_jobs_without_auth():
    """Test that job endpoints require authentication."""
    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        # Test list endpoint
        response = await client.get("/api/jobs")
        assert response.status_code == 401
        
        # Test get by ID endpoint
        fake_job_id = "00000000-0000-0000-0000-000000000000"
        response = await client.get(f"/api/jobs/{fake_job_id}")
        assert response.status_code == 401


@pytest.mark.asyncio
async def test_cancel_job(
    async_client: httpx.AsyncClient,
    auth_headers: Dict[str, str]
):
    """Test cancelling a job (if we can create a long-running one)."""
    # This test is tricky because we need a job that stays in pending/running state
    # For now, we'll test that the endpoint exists and handles invalid cases properly
    
    fake_job_id = "00000000-0000-0000-0000-000000000000"
    
    response = await async_client.post(
        f"/api/jobs/{fake_job_id}/cancel",
        headers=auth_headers
    )
    
    # Should get 404 for non-existent job
    assert response.status_code == 404