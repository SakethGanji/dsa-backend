"""Integration tests for complete workflows."""

import pytest
from httpx import AsyncClient
import time
from tests.utils import APITestClient, create_test_commit


@pytest.mark.integration
class TestCompleteWorkflow:
    """Test complete user workflows."""
    
    async def test_full_dataset_lifecycle(self, client: AsyncClient, db_session):
        """Test complete dataset lifecycle from creation to data retrieval."""
        api = APITestClient(client)
        
        # Step 1: Create admin user
        await db_session.execute("""
            INSERT INTO dsa_auth.roles (role_name, description) 
            VALUES ('admin', 'Administrator role')
            ON CONFLICT (role_name) DO NOTHING
        """)
        
        admin_user = await db_session.fetchrow("""
            INSERT INTO dsa_auth.users (soeid, password_hash, role_id)
            SELECT 'ADMIN01', $1, id FROM dsa_auth.roles WHERE role_name = 'admin'
            RETURNING id, soeid
        """, "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewYpfQaXUIjj9ye.")  # "password"
        
        # Step 2: Login as admin
        login_response = await api.login("ADMIN01", "password")
        assert "access_token" in login_response
        
        # Step 3: Create a dataset
        dataset_name = f"integration_test_{int(time.time())}"
        dataset = await api.create_dataset(
            name=dataset_name,
            description="Integration test dataset"
        )
        assert "dataset_id" in dataset
        dataset_id = dataset["dataset_id"]
        
        # Step 4: Create another user
        normal_user = await db_session.fetchrow("""
            INSERT INTO dsa_auth.users (soeid, password_hash, role_id)
            SELECT 'USER001', $1, id FROM dsa_auth.roles WHERE role_name = 'admin'
            RETURNING id, soeid
        """, "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewYpfQaXUIjj9ye.")
        
        # Step 5: Grant read permission to the other user
        permission_response = await api.grant_permission(
            dataset_id=dataset_id,
            user_id=normal_user["id"],
            permission_type="read"
        )
        assert permission_response["permission_type"] == "read"
        
        # Step 6: Create a commit with data
        commit_id = await create_test_commit(
            db_session,
            dataset_id=dataset_id,
            author_id=admin_user["id"]
        )
        
        # Step 7: Verify the dataset has data
        # (This would require implementing data retrieval endpoints)
        
        # Step 8: Login as normal user and verify read access
        await api.login("USER001", "password")
        # Would test data retrieval here
    
    async def test_permission_hierarchy(self, client: AsyncClient, db_session, test_user: dict):
        """Test permission hierarchy (admin > write > read)."""
        api = APITestClient(client)
        
        # Create users with different permission levels
        users = {}
        for perm_type in ["read", "write", "admin"]:
            user = await db_session.fetchrow("""
                INSERT INTO dsa_auth.users (soeid, password_hash, role_id)
                VALUES ($1, $2, $3)
                RETURNING id, soeid
            """, f"{perm_type.upper()}USR", "dummy_hash", test_user["role_id"])
            users[perm_type] = user
        
        # Create dataset
        dataset_id = await db_session.fetchval("""
            INSERT INTO dsa_core.datasets (name, description, created_by)
            VALUES ('perm_test_dataset', 'Permission hierarchy test', $1)
            RETURNING id
        """, test_user["id"])
        
        # Create ref
        await db_session.execute("""
            INSERT INTO dsa_core.refs (dataset_id, name, commit_id)
            VALUES ($1, 'main', NULL)
        """, dataset_id)
        
        # Grant different permissions
        for perm_type, user in users.items():
            await db_session.execute("""
                INSERT INTO dsa_auth.dataset_permissions (dataset_id, user_id, permission_type)
                VALUES ($1, $2, $3::dsa_auth.dataset_permission)
            """, dataset_id, user["id"], perm_type)
        
        # Verify permission checks
        from src.core.services.postgres import PostgresDatasetRepository
        dataset_repo = PostgresDatasetRepository(db_session)
        
        # Read user should have read access only
        assert await dataset_repo.check_user_permission(dataset_id, users["read"]["id"], "read") == True
        assert await dataset_repo.check_user_permission(dataset_id, users["read"]["id"], "write") == False
        assert await dataset_repo.check_user_permission(dataset_id, users["read"]["id"], "admin") == False
        
        # Write user should have read and write access
        assert await dataset_repo.check_user_permission(dataset_id, users["write"]["id"], "read") == True
        assert await dataset_repo.check_user_permission(dataset_id, users["write"]["id"], "write") == True
        assert await dataset_repo.check_user_permission(dataset_id, users["write"]["id"], "admin") == False
        
        # Admin user should have all access
        assert await dataset_repo.check_user_permission(dataset_id, users["admin"]["id"], "read") == True
        assert await dataset_repo.check_user_permission(dataset_id, users["admin"]["id"], "write") == True
        assert await dataset_repo.check_user_permission(dataset_id, users["admin"]["id"], "admin") == True
    
    async def test_concurrent_operations(self, client: AsyncClient, auth_headers: dict):
        """Test concurrent dataset operations."""
        import asyncio
        
        # Create multiple datasets concurrently
        tasks = []
        for i in range(5):
            task = client.post(
                "/api/datasets/",
                json={
                    "name": f"concurrent_dataset_{i}_{time.time()}",
                    "description": f"Concurrent test {i}"
                },
                headers=auth_headers
            )
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks)
        
        # All should succeed
        for response in responses:
            assert response.status_code == 200
            assert "dataset_id" in response.json()
    
    @pytest.mark.slow
    async def test_large_dataset_creation(self, db_session, test_user: dict):
        """Test creating a dataset with many rows."""
        import hashlib
        import json
        
        # Create dataset
        dataset_id = await db_session.fetchval("""
            INSERT INTO dsa_core.datasets (name, description, created_by)
            VALUES ('large_dataset', 'Large dataset test', $1)
            RETURNING id
        """, test_user["id"])
        
        # Create ref
        await db_session.execute("""
            INSERT INTO dsa_core.refs (dataset_id, name, commit_id)
            VALUES ($1, 'main', NULL)
        """, dataset_id)
        
        # Prepare large number of rows
        num_rows = 1000
        rows = []
        manifest = []
        
        for i in range(num_rows):
            data = {"id": i, "value": f"test_value_{i}"}
            row_hash = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
            rows.append((row_hash, json.dumps(data)))
            manifest.append((f"default:{i}", row_hash))
        
        # Bulk insert rows
        await db_session.copy_records_to_table(
            'dsa_core.rows',
            records=rows,
            columns=['row_hash', 'data']
        )
        
        # Create commit
        commit_data = {
            "dataset_id": dataset_id,
            "parent_commit_id": None,
            "manifest": manifest,
            "message": "Large dataset commit",
            "author_id": test_user["id"]
        }
        
        commit_id = hashlib.sha256(
            json.dumps(commit_data, sort_keys=True).encode()
        ).hexdigest()
        
        await db_session.execute("""
            INSERT INTO dsa_core.commits (commit_id, dataset_id, parent_commit_id, message, author_id)
            VALUES ($1, $2, $3, $4, $5)
        """, commit_id, dataset_id, None, "Large dataset commit", test_user["id"])
        
        # Bulk insert manifest
        manifest_records = [(commit_id, lid, rh) for lid, rh in manifest]
        await db_session.copy_records_to_table(
            'dsa_core.commit_rows',
            records=manifest_records,
            columns=['commit_id', 'logical_row_id', 'row_hash']
        )
        
        # Verify row count
        row_count = await db_session.fetchval("""
            SELECT COUNT(*) FROM dsa_core.commit_rows WHERE commit_id = $1
        """, commit_id)
        
        assert row_count == num_rows