#!/usr/bin/env python3
"""Simple API integration tests."""

import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(project_root, 'src'))

from fastapi.testclient import TestClient
from unittest.mock import Mock, AsyncMock, MagicMock
import json

# Mock the dependencies before importing the app
import core.dependencies as deps

# Create mock dependencies
mock_db_pool = Mock()
mock_parser_factory = Mock()
mock_stats_calculator = Mock()

deps._db_pool = mock_db_pool
deps._parser_factory = mock_parser_factory
deps._stats_calculator = mock_stats_calculator

# Import app after setting up mocks
from main import app

# Create test client
client = TestClient(app)


def test_root_endpoint():
    """Test root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "DSA Platform API v2.0"}


def test_health_check():
    """Test health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["version"] == "2.0.0"


def test_table_endpoints_require_auth():
    """Test that table endpoints require authentication."""
    # Without auth token
    response = client.get("/api/datasets/1/refs/main/tables")
    assert response.status_code == 401
    
    response = client.get("/api/datasets/1/refs/main/tables/primary/data")
    assert response.status_code == 401
    
    response = client.get("/api/datasets/1/refs/main/tables/primary/schema")
    assert response.status_code == 401


def test_versioning_endpoints_structure():
    """Test that versioning endpoints are registered correctly."""
    # Get OpenAPI schema
    response = client.get("/openapi.json")
    assert response.status_code == 200
    
    openapi = response.json()
    paths = openapi["paths"]
    
    # Check table endpoints exist
    assert "/api/datasets/{dataset_id}/refs/{ref_name}/tables" in paths
    assert "/api/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/data" in paths
    assert "/api/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/schema" in paths
    
    # Check commit endpoints exist
    assert "/api/datasets/{dataset_id}/commits/{commit_id}/schema" in paths
    assert "/api/datasets/{dataset_id}/refs/{ref_name}/data" in paths
    
    print("✅ All versioning endpoints are registered correctly!")


def main():
    """Run all tests."""
    print("=" * 60)
    print("🧪 DSA API Tests")
    print("=" * 60)
    
    try:
        test_root_endpoint()
        print("✅ Root endpoint test passed")
        
        test_health_check()
        print("✅ Health check test passed")
        
        test_table_endpoints_require_auth()
        print("✅ Authentication requirement tests passed")
        
        test_versioning_endpoints_structure()
        
        print("\n" + "=" * 60)
        print("🎉 All API tests passed!")
        print("=" * 60)
        return 0
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())