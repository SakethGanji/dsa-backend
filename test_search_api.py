#!/usr/bin/env python3
"""
Test script for the Unified Dataset Search API
"""

import asyncio
import httpx
import json
from datetime import datetime, timedelta

# Configuration
BASE_URL = "http://localhost:8000"
AUTH_TOKEN = None  # Set this after login

async def login(client: httpx.AsyncClient, username: str, password: str) -> str:
    """Login and get auth token"""
    response = await client.post(
        f"{BASE_URL}/api/users/token",
        data={"username": username, "password": password}
    )
    if response.status_code == 200:
        data = response.json()
        return data["access_token"]
    else:
        print(f"Login failed: {response.status_code} - {response.text}")
        return None

async def test_search_post(client: httpx.AsyncClient):
    """Test POST search endpoint"""
    print("\n=== Testing POST /api/datasets/search ===")
    
    # Test 1: Basic search
    search_request = {
        "query": "sales",
        "limit": 5,
        "include_facets": True
    }
    
    response = await client.post(
        f"{BASE_URL}/api/datasets/search",
        json=search_request,
        headers={"Authorization": f"Bearer {AUTH_TOKEN}"}
    )
    
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Found {data['total']} datasets")
        print(f"Execution time: {data['execution_time_ms']}ms")
        if data['results']:
            print("\nFirst result:")
            print(json.dumps(data['results'][0], indent=2))
        if data.get('facets'):
            print("\nFacets:")
            print(json.dumps(data['facets'], indent=2))
    else:
        print(f"Error: {response.text}")
    
    # Test 2: Advanced search with filters
    print("\n--- Testing advanced search with filters ---")
    advanced_search = {
        "query": "data",
        "tags": ["finance"],
        "file_types": ["parquet"],
        "created_at": {
            "start": (datetime.now() - timedelta(days=365)).isoformat(),
            "end": datetime.now().isoformat()
        },
        "fuzzy_search": True,
        "sort_by": "updated_at",
        "sort_order": "desc",
        "limit": 10
    }
    
    response = await client.post(
        f"{BASE_URL}/api/datasets/search",
        json=advanced_search,
        headers={"Authorization": f"Bearer {AUTH_TOKEN}"}
    )
    
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Found {data['total']} datasets matching advanced criteria")

async def test_search_get(client: httpx.AsyncClient):
    """Test GET search endpoint"""
    print("\n=== Testing GET /api/datasets/search ===")
    
    params = {
        "query": "test",
        "tags": ["sample", "test"],
        "fuzzy": "true",
        "limit": 5
    }
    
    response = await client.get(
        f"{BASE_URL}/api/datasets/search",
        params=params,
        headers={"Authorization": f"Bearer {AUTH_TOKEN}"}
    )
    
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Found {data['total']} datasets")
        print(f"Has more results: {data['has_more']}")

async def test_suggestions(client: httpx.AsyncClient):
    """Test search suggestions endpoint"""
    print("\n=== Testing POST /api/datasets/search/suggest ===")
    
    suggest_request = {
        "query": "sal",
        "limit": 5,
        "types": ["dataset_name", "tag"]
    }
    
    response = await client.post(
        f"{BASE_URL}/api/datasets/search/suggest",
        json=suggest_request,
        headers={"Authorization": f"Bearer {AUTH_TOKEN}"}
    )
    
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Got {len(data['suggestions'])} suggestions")
        for suggestion in data['suggestions']:
            print(f"  - {suggestion['text']} ({suggestion['type']}) - score: {suggestion['score']}")

async def test_search_init(client: httpx.AsyncClient):
    """Test search initialization (admin only)"""
    print("\n=== Testing POST /api/datasets/search/init ===")
    
    response = await client.post(
        f"{BASE_URL}/api/datasets/search/init",
        headers={"Authorization": f"Bearer {AUTH_TOKEN}"}
    )
    
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Result: {data}")
    else:
        print(f"Error: {response.text}")

async def main():
    """Run all tests"""
    async with httpx.AsyncClient() as client:
        # Login first
        global AUTH_TOKEN
        AUTH_TOKEN = await login(client, "admin", "admin_password")  # Update credentials
        
        if not AUTH_TOKEN:
            print("Failed to authenticate. Exiting.")
            return
        
        print("Successfully authenticated!")
        
        # Initialize search (if admin)
        await test_search_init(client)
        
        # Run search tests
        await test_search_post(client)
        await test_search_get(client)
        await test_suggestions(client)
        
        print("\n=== All tests completed ===")

if __name__ == "__main__":
    asyncio.run(main())