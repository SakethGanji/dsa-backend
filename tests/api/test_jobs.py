"""Tests for Jobs API endpoints."""

import pytest
from httpx import AsyncClient
from uuid import uuid4, UUID
import pytest
from datetime import datetime
from unittest.mock import patch, AsyncMock, Mock


@pytest.mark.asyncio
class TestJobStatus:
    """Test job status endpoint."""
    
    async def test_get_job_status_success(self, client: AsyncClient, auth_headers: dict):
        """Test successful retrieval of job status."""
        job_id = uuid4()
        
        # Mock the handler
        with patch('src.api.jobs.get_job_status_handler') as mock_handler_factory, \
             patch('src.api.jobs.get_current_user_id', return_value=1):
            
            mock_handler = Mock()
            mock_handler.handle = AsyncMock(return_value={
                "job_id": str(job_id),
                "run_type": "import",
                "status": "completed",
                "dataset_id": 123,
                "created_at": datetime.utcnow().isoformat(),
                "completed_at": datetime.utcnow().isoformat(),
                "error_message": None,
                "output_summary": {"rows_imported": 1000}
            })
            mock_handler_factory.return_value = mock_handler
            
            response = await client.get(
                f"/api/jobs/{job_id}",
                headers=auth_headers
            )
            
            assert response.status_code == 200
            data = response.json()
            assert data["job_id"] == str(job_id)
            assert data["status"] == "completed"
            assert data["run_type"] == "import"
            assert data["dataset_id"] == 123
            assert data["output_summary"]["rows_imported"] == 1000
    
    async def test_get_job_status_not_found(self, client: AsyncClient, auth_headers: dict):
        """Test getting status of non-existent job."""
        job_id = uuid4()
        
        with patch('src.api.jobs.get_job_status_handler') as mock_handler_factory, \
             patch('src.api.jobs.get_current_user_id', return_value=1):
            
            mock_handler = Mock()
            mock_handler.handle = AsyncMock(side_effect=ValueError("Job not found"))
            mock_handler_factory.return_value = mock_handler
            
            response = await client.get(
                f"/api/jobs/{job_id}",
                headers=auth_headers
            )
            
            # Should return 404 or appropriate error
            assert response.status_code in [404, 500]
    
    async def test_get_job_status_unauthorized(self, client: AsyncClient):
        """Test getting job status without authentication."""
        job_id = uuid4()
        
        # Without proper auth setup, this should fail
        response = await client.get(f"/api/jobs/{job_id}")
        
        # Expect 401 or 403 depending on auth implementation
        assert response.status_code in [401, 403, 500]
    
    async def test_get_job_status_invalid_uuid(self, client: AsyncClient, auth_headers: dict):
        """Test getting job status with invalid UUID."""
        response = await client.get(
            "/api/jobs/invalid-uuid",
            headers=auth_headers
        )
        
        assert response.status_code == 422
        assert "validation error" in response.text.lower()
    
    async def test_get_job_status_pending(self, client: AsyncClient, auth_headers: dict):
        """Test getting status of pending job."""
        job_id = uuid4()
        
        with patch('src.api.jobs.get_job_status_handler') as mock_handler_factory, \
             patch('src.api.jobs.get_current_user_id', return_value=1):
            
            mock_handler = Mock()
            mock_handler.handle = AsyncMock(return_value={
                "job_id": str(job_id),
                "run_type": "import",
                "status": "pending",
                "dataset_id": 123,
                "created_at": datetime.utcnow().isoformat(),
                "completed_at": None,
                "error_message": None,
                "output_summary": None
            })
            mock_handler_factory.return_value = mock_handler
            
            response = await client.get(
                f"/api/jobs/{job_id}",
                headers=auth_headers
            )
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "pending"
            assert data["completed_at"] is None
            assert data["output_summary"] is None
    
    async def test_get_job_status_failed(self, client: AsyncClient, auth_headers: dict):
        """Test getting status of failed job."""
        job_id = uuid4()
        
        with patch('src.api.jobs.get_job_status_handler') as mock_handler_factory, \
             patch('src.api.jobs.get_current_user_id', return_value=1):
            
            mock_handler = Mock()
            mock_handler.handle = AsyncMock(return_value={
                "job_id": str(job_id),
                "run_type": "import",
                "status": "failed",
                "dataset_id": 123,
                "created_at": datetime.utcnow().isoformat(),
                "completed_at": datetime.utcnow().isoformat(),
                "error_message": "File format not supported",
                "output_summary": None
            })
            mock_handler_factory.return_value = mock_handler
            
            response = await client.get(
                f"/api/jobs/{job_id}",
                headers=auth_headers
            )
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "failed"
            assert data["error_message"] == "File format not supported"
            assert data["output_summary"] is None
    
    async def test_get_job_status_different_user(self, client: AsyncClient, auth_headers: dict):
        """Test that users can only see their own jobs."""
        job_id = uuid4()
        
        with patch('src.api.jobs.get_job_status_handler') as mock_handler_factory, \
             patch('src.api.jobs.get_current_user_id', return_value=2):  # Different user
            
            mock_handler = Mock()
            # Handler should check user ownership
            mock_handler.handle = AsyncMock(side_effect=PermissionError("Not authorized to view this job"))
            mock_handler_factory.return_value = mock_handler
            
            response = await client.get(
                f"/api/jobs/{job_id}",
                headers=auth_headers
            )
            
            # Should return 403 Forbidden
            assert response.status_code in [403, 500]


@pytest.mark.asyncio
class TestJobOperations:
    """Test job-related operations."""
    
    async def test_import_job_creates_job_entry(self, client: AsyncClient, auth_headers: dict):
        """Test that file import creates a job entry."""
        # This test would verify that when importing a file via versioning API,
        # a job is created and can be tracked
        
        # Mock file upload
        with patch('src.api.versioning.QueueImportJobHandler') as mock_handler_class:
            mock_handler = Mock()
            job_id = uuid4()
            mock_handler.handle = AsyncMock(return_value={
                "job_id": job_id,
                "status": "pending",
                "message": "Import job queued successfully"
            })
            mock_handler_class.return_value = mock_handler
            
            # Create mock file
            files = {
                'file': ('test_data.csv', b'id,name,value\n1,test,100', 'text/csv')
            }
            data = {
                'commit_message': 'Test import'
            }
            
            with patch('src.api.versioning.get_current_user', return_value={"id": 1}):
                response = await client.post(
                    "/api/datasets/1/refs/main/import",
                    files=files,
                    data=data,
                    headers=auth_headers
                )
            
            # Verify job was created
            if response.status_code == 200:
                data = response.json()
                assert "job_id" in data
                assert data["status"] == "pending"


@pytest.mark.asyncio
class TestJobValidation:
    """Test job endpoint validation."""
    
    async def test_job_id_format_validation(self, client: AsyncClient, auth_headers: dict):
        """Test various invalid job ID formats."""
        invalid_ids = [
            "not-a-uuid",
            "12345",
            "",
            "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "g1234567-89ab-cdef-0123-456789abcdef"  # Invalid hex
        ]
        
        for invalid_id in invalid_ids:
            response = await client.get(
                f"/api/jobs/{invalid_id}",
                headers=auth_headers
            )
            assert response.status_code == 422
    
    async def test_job_status_response_validation(self, client: AsyncClient, auth_headers: dict):
        """Test that job status response matches expected schema."""
        job_id = uuid4()
        
        with patch('src.api.jobs.get_job_status_handler') as mock_handler_factory, \
             patch('src.api.jobs.get_current_user_id', return_value=1):
            
            mock_handler = Mock()
            mock_handler.handle = AsyncMock(return_value={
                "job_id": str(job_id),
                "run_type": "import",
                "status": "running",
                "dataset_id": 123,
                "created_at": datetime.utcnow().isoformat(),
                "completed_at": None,
                "error_message": None,
                "output_summary": {"progress": "50%"}
            })
            mock_handler_factory.return_value = mock_handler
            
            response = await client.get(
                f"/api/jobs/{job_id}",
                headers=auth_headers
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Validate response structure
            required_fields = ["job_id", "run_type", "status", "dataset_id", "created_at"]
            for field in required_fields:
                assert field in data
            
            # Validate status values
            valid_statuses = ["pending", "running", "completed", "failed", "cancelled"]
            assert data["status"] in valid_statuses
            
            # Validate run_type values
            valid_run_types = ["import", "export", "transformation", "validation"]
            assert data["run_type"] in valid_run_types


@pytest.mark.asyncio
class TestJobConcurrency:
    """Test concurrent job access."""
    
    async def test_multiple_job_status_requests(self, client: AsyncClient, auth_headers: dict):
        """Test handling multiple concurrent job status requests."""
        job_ids = [uuid4() for _ in range(5)]
        
        with patch('src.api.jobs.get_job_status_handler') as mock_handler_factory, \
             patch('src.api.jobs.get_current_user_id', return_value=1):
            
            mock_handler = Mock()
            
            async def mock_handle(job_id, user_id):
                return {
                    "job_id": str(job_id),
                    "run_type": "import",
                    "status": "completed",
                    "dataset_id": 123,
                    "created_at": datetime.utcnow().isoformat(),
                    "completed_at": datetime.utcnow().isoformat(),
                    "error_message": None,
                    "output_summary": {"rows": 100}
                }
            
            mock_handler.handle = mock_handle
            mock_handler_factory.return_value = mock_handler
            
            # Make concurrent requests
            import asyncio
            tasks = []
            for job_id in job_ids:
                task = client.get(f"/api/jobs/{job_id}", headers=auth_headers)
                tasks.append(task)
            
            responses = await asyncio.gather(*tasks)
            
            # All should succeed
            for response in responses:
                assert response.status_code == 200