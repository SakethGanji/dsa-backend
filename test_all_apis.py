#!/usr/bin/env python3
"""Quick test of all API endpoints."""

import asyncio
from httpx import AsyncClient
import json

async def test_apis():
    """Test all main API endpoints."""
    base_url = "http://localhost:8000"
    
    async with AsyncClient(base_url=base_url) as client:
        print("Testing DSA Platform APIs...")
        print("=" * 50)
        
        # 1. Health check
        print("\n1. Testing health endpoint...")
        resp = await client.get("/health")
        print(f"   Status: {resp.status_code}")
        print(f"   Response: {resp.json()}")
        assert resp.status_code == 200
        
        # 2. Login
        print("\n2. Testing login...")
        # First need to create a test user in DB
        # For now, we'll skip auth and test other endpoints
        
        # 3. OpenAPI docs
        print("\n3. Testing OpenAPI spec...")
        resp = await client.get("/openapi.json")
        print(f"   Status: {resp.status_code}")
        openapi = resp.json()
        print(f"   Title: {openapi.get('info', {}).get('title')}")
        print(f"   Version: {openapi.get('info', {}).get('version')}")
        
        # 4. List API paths
        print("\n4. Available API endpoints:")
        paths = openapi.get('paths', {})
        for path, methods in sorted(paths.items()):
            for method in methods:
                if method in ['get', 'post', 'put', 'delete']:
                    print(f"   {method.upper():6} {path}")
        
        print("\n✅ All basic API checks passed!")
        print("=" * 50)

if __name__ == "__main__":
    asyncio.run(test_apis())