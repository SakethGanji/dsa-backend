"""End-to-end tests for versioning functionality."""

import pytest
from httpx import AsyncClient
import json


@pytest.mark.asyncio
async def test_complete_versioning_workflow(client: AsyncClient, auth_headers: dict, db_session):
    """Test complete workflow: create dataset, add commits, view history, checkout."""
    
    # Step 1: Create a dataset
    create_response = await client.post(
        "/api/datasets/",
        json={
            "name": "Test Versioning Dataset",
            "description": "Dataset for testing versioning",
            "tags": ["test", "versioning"]
        },
        headers=auth_headers
    )
    assert create_response.status_code == 200
    dataset = create_response.json()
    dataset_id = dataset["dataset_id"]
    
    try:
        # Step 2: Create initial commit with data
        initial_data = [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200}
        ]
        
        commit1_response = await client.post(
            f"/api/datasets/{dataset_id}/refs/main/commits",
            json={
                "message": "Initial data import",
                "data": initial_data
            },
            headers=auth_headers
        )
        assert commit1_response.status_code == 200
        commit1 = commit1_response.json()
        commit1_id = commit1["commit_id"]
        
        # Step 3: Create second commit with updated data
        updated_data = [
            {"id": 1, "name": "Alice", "value": 150},  # Updated value
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 300}  # New row
        ]
        
        commit2_response = await client.post(
            f"/api/datasets/{dataset_id}/refs/main/commits",
            json={
                "message": "Updated Alice value and added Charlie",
                "data": updated_data
            },
            headers=auth_headers
        )
        assert commit2_response.status_code == 200
        commit2 = commit2_response.json()
        commit2_id = commit2["commit_id"]
        
        # Step 4: View commit history
        history_response = await client.get(
            f"/api/datasets/{dataset_id}/history",
            headers=auth_headers
        )
        assert history_response.status_code == 200
        history = history_response.json()
        
        # Verify history
        assert history["total"] == 2
        assert len(history["commits"]) == 2
        
        # Most recent commit first
        assert history["commits"][0]["commit_id"] == commit2_id
        assert history["commits"][0]["message"] == "Updated Alice value and added Charlie"
        assert history["commits"][0]["row_count"] == 3
        assert history["commits"][0]["parent_commit_id"] == commit1_id
        
        # Initial commit second
        assert history["commits"][1]["commit_id"] == commit1_id
        assert history["commits"][1]["message"] == "Initial data import"
        assert history["commits"][1]["row_count"] == 2
        assert history["commits"][1]["parent_commit_id"] is None
        
        # Step 5: Checkout first commit
        checkout1_response = await client.get(
            f"/api/datasets/{dataset_id}/commits/{commit1_id}/data",
            headers=auth_headers
        )
        assert checkout1_response.status_code == 200
        checkout1_data = checkout1_response.json()
        
        # Verify first commit data
        assert checkout1_data["commit_id"] == commit1_id
        assert checkout1_data["total_rows"] == 2
        assert len(checkout1_data["data"]) == 2
        
        # Check data content (note: order might vary)
        data_by_id = {row["id"]: row for row in checkout1_data["data"]}
        assert data_by_id[1]["name"] == "Alice"
        assert data_by_id[1]["value"] == 100  # Original value
        assert data_by_id[2]["name"] == "Bob"
        assert 3 not in data_by_id  # Charlie not in first commit
        
        # Step 6: Checkout second commit
        checkout2_response = await client.get(
            f"/api/datasets/{dataset_id}/commits/{commit2_id}/data",
            headers=auth_headers
        )
        assert checkout2_response.status_code == 200
        checkout2_data = checkout2_response.json()
        
        # Verify second commit data
        assert checkout2_data["commit_id"] == commit2_id
        assert checkout2_data["total_rows"] == 3
        assert len(checkout2_data["data"]) == 3
        
        # Check updated data
        data_by_id = {row["id"]: row for row in checkout2_data["data"]}
        assert data_by_id[1]["value"] == 150  # Updated value
        assert data_by_id[3]["name"] == "Charlie"  # New row
        
        # Step 7: Test pagination in history
        paginated_history_response = await client.get(
            f"/api/datasets/{dataset_id}/history?offset=1&limit=1",
            headers=auth_headers
        )
        assert paginated_history_response.status_code == 200
        paginated_history = paginated_history_response.json()
        assert len(paginated_history["commits"]) == 1
        assert paginated_history["commits"][0]["commit_id"] == commit1_id
        
        # Step 8: Test pagination in checkout
        paginated_checkout_response = await client.get(
            f"/api/datasets/{dataset_id}/commits/{commit2_id}/data?offset=1&limit=2",
            headers=auth_headers
        )
        assert paginated_checkout_response.status_code == 200
        paginated_checkout = paginated_checkout_response.json()
        assert len(paginated_checkout["data"]) == 2
        assert paginated_checkout["offset"] == 1
        assert paginated_checkout["limit"] == 2
        
    finally:
        # Cleanup: Delete the dataset
        await client.delete(
            f"/api/datasets/{dataset_id}",
            headers=auth_headers
        )


@pytest.mark.asyncio
async def test_versioning_with_multi_table_data(client: AsyncClient, auth_headers: dict):
    """Test versioning with multi-table datasets (e.g., Excel sheets)."""
    
    # Create dataset
    create_response = await client.post(
        "/api/datasets/",
        json={
            "name": "Multi-table Test Dataset",
            "description": "Testing multi-table versioning"
        },
        headers=auth_headers
    )
    assert create_response.status_code == 200
    dataset_id = create_response.json()["dataset_id"]
    
    try:
        # Create commit with multi-table data
        # Simulating Excel-like structure with Revenue and Expenses sheets
        multi_table_data = [
            # Revenue sheet data
            {"_sheet": "Revenue", "month": "Jan", "amount": 1000},
            {"_sheet": "Revenue", "month": "Feb", "amount": 1200},
            # Expenses sheet data
            {"_sheet": "Expenses", "category": "Salaries", "amount": 800},
            {"_sheet": "Expenses", "category": "Rent", "amount": 200}
        ]
        
        commit_response = await client.post(
            f"/api/datasets/{dataset_id}/refs/main/commits",
            json={
                "message": "Initial multi-table data",
                "data": multi_table_data
            },
            headers=auth_headers
        )
        assert commit_response.status_code == 200
        commit_id = commit_response.json()["commit_id"]
        
        # Test checkout with table filter
        revenue_response = await client.get(
            f"/api/datasets/{dataset_id}/commits/{commit_id}/data?table_key=Revenue",
            headers=auth_headers
        )
        assert revenue_response.status_code == 200
        revenue_data = revenue_response.json()
        
        # Should only get Revenue data
        assert all("Revenue" in row["_logical_row_id"] for row in revenue_data["data"])
        
    finally:
        # Cleanup
        await client.delete(
            f"/api/datasets/{dataset_id}",
            headers=auth_headers
        )


@pytest.mark.asyncio 
async def test_empty_dataset_history(client: AsyncClient, auth_headers: dict):
    """Test history for dataset with no commits."""
    
    # Create dataset without any commits
    create_response = await client.post(
        "/api/datasets/",
        json={
            "name": "Empty Dataset",
            "description": "No commits yet"
        },
        headers=auth_headers
    )
    assert create_response.status_code == 200
    dataset_id = create_response.json()["dataset_id"]
    
    try:
        # Get history of empty dataset
        history_response = await client.get(
            f"/api/datasets/{dataset_id}/history",
            headers=auth_headers
        )
        assert history_response.status_code == 200
        history = history_response.json()
        
        # Should have no commits
        assert history["total"] == 0
        assert len(history["commits"]) == 0
        
    finally:
        # Cleanup
        await client.delete(
            f"/api/datasets/{dataset_id}",
            headers=auth_headers
        )


@pytest.mark.asyncio
async def test_checkout_nonexistent_commit(client: AsyncClient, auth_headers: dict):
    """Test checkout of non-existent commit."""
    
    # Create dataset
    create_response = await client.post(
        "/api/datasets/",
        json={"name": "Test Dataset"},
        headers=auth_headers
    )
    assert create_response.status_code == 200
    dataset_id = create_response.json()["dataset_id"]
    
    try:
        # Try to checkout non-existent commit
        checkout_response = await client.get(
            f"/api/datasets/{dataset_id}/commits/nonexistent123/data",
            headers=auth_headers
        )
        assert checkout_response.status_code == 404
        assert "Commit not found" in checkout_response.json()["detail"]
        
    finally:
        # Cleanup
        await client.delete(
            f"/api/datasets/{dataset_id}",
            headers=auth_headers
        )