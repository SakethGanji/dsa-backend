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


async def test_download_dataset_excel(async_client: AsyncClient, created_dataset, auth_headers):
    """Test downloading entire dataset as Excel."""
    dataset_id = created_dataset["dataset_id"]
    
    response = await async_client.get(
        f"/api/datasets/{dataset_id}/refs/main/download?format=excel",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_200_OK
    assert response.headers["content-type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


async def test_download_dataset_json(async_client: AsyncClient, created_dataset, auth_headers):
    """Test downloading entire dataset as JSON."""
    dataset_id = created_dataset["dataset_id"]
    
    response = await async_client.get(
        f"/api/datasets/{dataset_id}/refs/main/download?format=json",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_200_OK
    assert response.headers["content-type"] == "application/json"


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


async def test_download_table_with_columns(async_client: AsyncClient, created_dataset, auth_headers):
    """Test downloading table with specific columns."""
    dataset_id = created_dataset["dataset_id"]
    
    response = await async_client.get(
        f"/api/datasets/{dataset_id}/refs/main/tables/primary/download?format=csv&columns=id,name",
        headers=auth_headers
    )
    
    assert response.status_code == status.HTTP_200_OK
    assert response.headers["content-type"].startswith("text/csv")
    # Could parse CSV to verify only requested columns are included


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