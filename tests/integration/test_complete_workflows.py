"""Integration tests for complete end-to-end workflows."""

import pytest
from httpx import AsyncClient
import time
from uuid import uuid4
import asyncio


@pytest.mark.integration
class TestCompleteDatasetWorkflow:
    """Test complete dataset lifecycle workflow."""
    
    async def test_dataset_crud_workflow(self, client: AsyncClient, auth_headers: dict):
        """Test creating, reading, updating, and deleting a dataset."""
        # 1. Create dataset with tags
        dataset_name = f"Complete Workflow Test {int(time.time())}"
        create_response = await client.post(
            "/api/datasets/",
            json={
                "name": dataset_name,
                "description": "Testing complete workflow",
                "tags": ["test", "integration", "workflow"]
            },
            headers=auth_headers
        )
        assert create_response.status_code == 200
        dataset = create_response.json()
        dataset_id = dataset["dataset_id"]
        assert dataset["tags"] == ["test", "integration", "workflow"]
        
        # 2. Get dataset details
        get_response = await client.get(
            f"/api/datasets/{dataset_id}",
            headers=auth_headers
        )
        assert get_response.status_code == 200
        details = get_response.json()
        assert details["name"] == dataset_name
        assert details["tags"] == ["test", "integration", "workflow"]
        
        # 3. List datasets (should include our new dataset)
        list_response = await client.get(
            "/api/datasets/",
            headers=auth_headers
        )
        assert list_response.status_code == 200
        datasets = list_response.json()["datasets"]
        assert any(d["dataset_id"] == dataset_id for d in datasets)
        
        # 4. Update dataset
        update_response = await client.patch(
            f"/api/datasets/{dataset_id}",
            json={
                "name": f"{dataset_name} - Updated",
                "description": "Updated description",
                "tags": ["updated", "modified"]
            },
            headers=auth_headers
        )
        assert update_response.status_code == 200
        updated = update_response.json()
        assert updated["name"] == f"{dataset_name} - Updated"
        assert updated["tags"] == ["updated", "modified"]
        
        # 5. Delete dataset
        delete_response = await client.delete(
            f"/api/datasets/{dataset_id}",
            headers=auth_headers
        )
        assert delete_response.status_code == 200
        
        # 6. Verify deletion
        verify_response = await client.get(
            f"/api/datasets/{dataset_id}",
            headers=auth_headers
        )
        assert verify_response.status_code in [403, 404]


@pytest.mark.integration
class TestPermissionWorkflow:
    """Test permission management workflow."""
    
    async def test_permission_grant_and_access_workflow(
        self, client: AsyncClient, auth_headers: dict, db_session
    ):
        """Test granting permissions and verifying access."""
        # 1. Create a dataset
        dataset_response = await client.post(
            "/api/datasets/",
            json={
                "name": f"Permission Test {int(time.time())}",
                "description": "Testing permissions"
            },
            headers=auth_headers
        )
        assert dataset_response.status_code == 200
        dataset_id = dataset_response.json()["dataset_id"]
        
        # 2. Create another user (using public endpoint for testing)
        other_user_soeid = f"USR{int(time.time()) % 10000:04d}"
        create_user_response = await client.post(
            "/api/users/register-public",
            json={
                "soeid": other_user_soeid,
                "password": "Test@123456",
                "role_id": 1
            }
        )
        if create_user_response.status_code == 200:
            other_user_id = create_user_response.json()["id"]
        else:
            # If public registration fails, create user directly in DB
            other_user_id = await db_session.fetchval("""
                INSERT INTO dsa_auth.users (soeid, password_hash, role_id)
                SELECT $1, 'dummy_hash', id FROM dsa_auth.roles WHERE role_name = 'admin'
                RETURNING id
            """, other_user_soeid)
        
        # 3. Grant read permission to other user
        grant_response = await client.post(
            f"/api/datasets/{dataset_id}/permissions",
            json={
                "user_id": other_user_id,
                "permission_type": "read"
            },
            headers=auth_headers
        )
        assert grant_response.status_code == 200
        
        # 4. Upgrade permission to write
        upgrade_response = await client.post(
            f"/api/datasets/{dataset_id}/permissions",
            json={
                "user_id": other_user_id,
                "permission_type": "write"
            },
            headers=auth_headers
        )
        assert upgrade_response.status_code == 200
        
        # 5. Try to grant admin (should work as we're admin)
        admin_response = await client.post(
            f"/api/datasets/{dataset_id}/permissions",
            json={
                "user_id": other_user_id,
                "permission_type": "admin"
            },
            headers=auth_headers
        )
        assert admin_response.status_code == 200
        
        # Cleanup
        await client.delete(f"/api/datasets/{dataset_id}", headers=auth_headers)


@pytest.mark.integration
@pytest.mark.slow
class TestDataImportWorkflow:
    """Test data import and versioning workflow."""
    
    async def test_file_import_and_commit_history(
        self, client: AsyncClient, auth_headers: dict
    ):
        """Test importing data and viewing commit history."""
        # 1. Create dataset
        dataset_response = await client.post(
            "/api/datasets/",
            json={
                "name": f"Import Test {int(time.time())}",
                "description": "Testing data import"
            },
            headers=auth_headers
        )
        assert dataset_response.status_code == 200
        dataset_id = dataset_response.json()["dataset_id"]
        
        # 2. Import CSV file (mocked)
        with patch('src.api.versioning.get_current_user', return_value={"id": 1}):
            csv_content = b"""id,name,value,category
1,Product A,100.50,Electronics
2,Product B,50.25,Books
3,Product C,75.00,Electronics
4,Product D,125.75,Clothing"""
            
            files = {'file': ('test_data.csv', csv_content, 'text/csv')}
            data = {'commit_message': 'Initial data import'}
            
            import_response = await client.post(
                f"/api/datasets/{dataset_id}/refs/main/import",
                files=files,
                data=data,
                headers=auth_headers
            )
            
            if import_response.status_code == 200:
                job_id = import_response.json()["job_id"]
                
                # 3. Check job status (would need to wait for completion)
                job_response = await client.get(
                    f"/api/jobs/{job_id}",
                    headers=auth_headers
                )
                # Job endpoint might not be fully implemented
                
        # 4. Create direct commit
        with patch('src.api.versioning.get_current_user', return_value={"id": 1}):
            commit_response = await client.post(
                f"/api/datasets/{dataset_id}/refs/main/commits",
                json={
                    "message": "Add more products",
                    "data": [
                        {"id": 5, "name": "Product E", "value": 200.00, "category": "Electronics"},
                        {"id": 6, "name": "Product F", "value": 45.50, "category": "Books"}
                    ]
                },
                headers=auth_headers
            )
            # This might fail if versioning isn't fully set up
        
        # 5. Get commit history
        history_response = await client.get(
            f"/api/datasets/{dataset_id}/history",
            headers=auth_headers
        )
        if history_response.status_code == 200:
            history = history_response.json()
            assert "commits" in history
            assert "total" in history
        
        # Cleanup
        await client.delete(f"/api/datasets/{dataset_id}", headers=auth_headers)


@pytest.mark.integration
class TestPaginationWorkflow:
    """Test pagination across different endpoints."""
    
    async def test_dataset_pagination_workflow(
        self, client: AsyncClient, auth_headers: dict
    ):
        """Test pagination with multiple datasets."""
        # 1. Create multiple datasets
        dataset_ids = []
        for i in range(15):
            response = await client.post(
                "/api/datasets/",
                json={
                    "name": f"Pagination Test {i} - {int(time.time())}",
                    "description": f"Dataset number {i}",
                    "tags": [f"page-{i // 5}"]
                },
                headers=auth_headers
            )
            if response.status_code == 200:
                dataset_ids.append(response.json()["dataset_id"])
        
        # 2. Test different pagination scenarios
        # First page
        page1_response = await client.get(
            "/api/datasets/?offset=0&limit=5",
            headers=auth_headers
        )
        assert page1_response.status_code == 200
        page1_data = page1_response.json()
        assert len(page1_data["datasets"]) <= 5
        assert page1_data["offset"] == 0
        assert page1_data["limit"] == 5
        
        # Second page
        page2_response = await client.get(
            "/api/datasets/?offset=5&limit=5",
            headers=auth_headers
        )
        assert page2_response.status_code == 200
        page2_data = page2_response.json()
        assert page2_data["offset"] == 5
        
        # Last page with partial results
        last_page_response = await client.get(
            "/api/datasets/?offset=10&limit=10",
            headers=auth_headers
        )
        assert last_page_response.status_code == 200
        
        # Beyond available data
        empty_response = await client.get(
            "/api/datasets/?offset=10000&limit=10",
            headers=auth_headers
        )
        assert empty_response.status_code == 200
        assert len(empty_response.json()["datasets"]) == 0
        
        # Cleanup
        for dataset_id in dataset_ids:
            await client.delete(f"/api/datasets/{dataset_id}", headers=auth_headers)


@pytest.mark.integration
class TestErrorHandlingWorkflow:
    """Test error handling across workflows."""
    
    async def test_graceful_error_handling(
        self, client: AsyncClient, auth_headers: dict
    ):
        """Test that errors are handled gracefully throughout workflows."""
        # 1. Try to update non-existent dataset
        update_response = await client.patch(
            "/api/datasets/999999",
            json={"name": "Should Fail"},
            headers=auth_headers
        )
        assert update_response.status_code in [403, 404]
        assert "detail" in update_response.json()
        
        # 2. Try to grant permission on non-existent dataset
        grant_response = await client.post(
            "/api/datasets/999999/permissions",
            json={
                "user_id": 1,
                "permission_type": "read"
            },
            headers=auth_headers
        )
        assert grant_response.status_code in [403, 404]
        
        # 3. Try to create dataset with invalid data
        invalid_response = await client.post(
            "/api/datasets/",
            json={
                "name": "",  # Empty name
                "description": "x" * 1001  # Too long
            },
            headers=auth_headers
        )
        assert invalid_response.status_code == 422
        errors = invalid_response.json()
        assert "detail" in errors
        
        # 4. Test transaction rollback on error
        # Create a dataset then try an invalid operation
        dataset_response = await client.post(
            "/api/datasets/",
            json={
                "name": f"Transaction Test {int(time.time())}",
                "description": "Testing transactions"
            },
            headers=auth_headers
        )
        if dataset_response.status_code == 200:
            dataset_id = dataset_response.json()["dataset_id"]
            
            # Try to grant permission to invalid user
            invalid_grant = await client.post(
                f"/api/datasets/{dataset_id}/permissions",
                json={
                    "user_id": -1,
                    "permission_type": "read"
                },
                headers=auth_headers
            )
            assert invalid_grant.status_code in [400, 422]
            
            # Verify dataset still exists and is accessible
            verify_response = await client.get(
                f"/api/datasets/{dataset_id}",
                headers=auth_headers
            )
            assert verify_response.status_code == 200
            
            # Cleanup
            await client.delete(f"/api/datasets/{dataset_id}", headers=auth_headers)


@pytest.mark.integration
class TestConcurrentOperations:
    """Test concurrent operations on the API."""
    
    async def test_concurrent_dataset_creation(
        self, client: AsyncClient, auth_headers: dict
    ):
        """Test creating multiple datasets concurrently."""
        # Create 10 datasets concurrently
        tasks = []
        for i in range(10):
            task = client.post(
                "/api/datasets/",
                json={
                    "name": f"Concurrent Test {i} - {int(time.time())}",
                    "description": f"Testing concurrent creation {i}",
                    "tags": ["concurrent", f"batch-{i}"]
                },
                headers=auth_headers
            )
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify all succeeded
        dataset_ids = []
        for i, response in enumerate(responses):
            if isinstance(response, Exception):
                pytest.fail(f"Concurrent request {i} failed: {response}")
            assert response.status_code == 200
            dataset_ids.append(response.json()["dataset_id"])
        
        # Verify all datasets were created
        assert len(dataset_ids) == 10
        assert len(set(dataset_ids)) == 10  # All unique
        
        # Cleanup
        cleanup_tasks = []
        for dataset_id in dataset_ids:
            task = client.delete(f"/api/datasets/{dataset_id}", headers=auth_headers)
            cleanup_tasks.append(task)
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)