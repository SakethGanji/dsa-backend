"""Integration tests for download endpoints."""
import pytest
from httpx import AsyncClient
from fastapi import status

# Test download dataset endpoint
async def test_download_dataset_csv(async_client: AsyncClient, created_dataset, auth_headers):
    """Test downloading entire dataset as CSV."""
    dataset_id = created_dataset["dataset_id"]
    
    # Download as CSV
    response = await async_client.get(
        f"/api/datasets/{dataset_id}/refs/main/download?format=csv",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_200_OK
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment" in response.headers.get("content-disposition", "")


async def test_download_dataset_excel_not_supported(async_client: AsyncClient, created_dataset, auth_headers):
    """Test that Excel format is not yet supported."""
    dataset_id = created_dataset["dataset_id"]
    
    response = await async_client.get(
        f"/api/datasets/{dataset_id}/refs/main/download?format=excel",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_400_BAD_REQUEST  # ValueError becomes 400


async def test_download_dataset_json_not_supported(async_client: AsyncClient, created_dataset, auth_headers):
    """Test that JSON format is not yet supported."""
    dataset_id = created_dataset["dataset_id"]
    
    response = await async_client.get(
        f"/api/datasets/{dataset_id}/refs/main/download?format=json",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_400_BAD_REQUEST  # ValueError becomes 400


async def test_download_dataset_invalid_format(async_client: AsyncClient, created_dataset, auth_headers):
    """Test downloading with invalid format."""
    dataset_id = created_dataset["dataset_id"]
    
    response = await async_client.get(
        f"/api/datasets/{dataset_id}/refs/main/download?format=invalid",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_400_BAD_REQUEST  # Will be 400 due to ValueError


async def test_download_table_csv(async_client: AsyncClient, created_dataset, auth_headers):
    """Test downloading specific table as CSV."""
    dataset_id = created_dataset["dataset_id"]
    
    response = await async_client.get(
        f"/api/datasets/{dataset_id}/refs/main/tables/primary/download?format=csv",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_200_OK
    assert response.headers["content-type"].startswith("text/csv")


async def test_download_table_with_columns_not_supported(async_client: AsyncClient, created_dataset, auth_headers):
    """Test that column selection is not yet supported."""
    dataset_id = created_dataset["dataset_id"]
    
    response = await async_client.get(
        f"/api/datasets/{dataset_id}/refs/main/tables/primary/download?format=csv&columns=id,name",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_400_BAD_REQUEST  # ValueError becomes 400


async def test_download_nonexistent_dataset(async_client: AsyncClient, auth_headers):
    """Test downloading non-existent dataset."""
    response = await async_client.get(
        "/api/datasets/99999/refs/main/download?format=csv",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_404_NOT_FOUND


async def test_download_nonexistent_ref(async_client: AsyncClient, created_dataset, auth_headers):
    """Test downloading with non-existent ref."""
    dataset_id = created_dataset["dataset_id"]
    
    response = await async_client.get(
        f"/api/datasets/{dataset_id}/refs/nonexistent/download?format=csv",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_404_NOT_FOUND


async def test_download_large_dataset_streaming(async_client: AsyncClient, created_dataset, auth_headers):
    """Test that streaming download works efficiently for large datasets."""
    dataset_id = created_dataset["dataset_id"]
    
    # Start download
    response = await async_client.get(
        f"/api/datasets/{dataset_id}/refs/main/download?format=csv",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_200_OK
    assert response.headers["content-type"].startswith("text/csv")
    
    # Verify we can iterate through the response without loading all into memory
    total_size = 0
    async for chunk in response.aiter_bytes(chunk_size=8192):
        total_size += len(chunk)
        # In a real test, we could monitor memory usage here
        
    assert total_size > 0  # We got some data