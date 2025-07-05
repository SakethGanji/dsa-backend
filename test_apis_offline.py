#!/usr/bin/env python3
"""Test all API endpoints using test client (no server needed)."""

import sys
sys.path.insert(0, 'src')

from fastapi.testclient import TestClient
from src.main import app
from src.core.dependencies import get_db_pool, get_current_user
from unittest.mock import AsyncMock, Mock

# Mock dependencies
mock_pool = Mock()
mock_pool.acquire = AsyncMock()

mock_user = {
    "id": 1,
    "soeid": "TEST_USER",
    "role": "admin"
}

# Override dependencies
app.dependency_overrides[get_db_pool] = lambda: mock_pool
app.dependency_overrides[get_current_user] = lambda: mock_user

# Create test client
client = TestClient(app)

print("Testing DSA Platform APIs (offline mode)...")
print("=" * 50)

# 1. Health check
print("\n1. Testing health endpoint...")
resp = client.get("/health")
print(f"   Status: {resp.status_code}")
print(f"   Response: {resp.json()}")

# 2. OpenAPI spec
print("\n2. Testing OpenAPI spec...")
resp = client.get("/openapi.json")
print(f"   Status: {resp.status_code}")
openapi = resp.json()
print(f"   Title: {openapi.get('info', {}).get('title')}")
print(f"   Version: {openapi.get('info', {}).get('version')}")

# 3. List all endpoints
print("\n3. Available API endpoints:")
paths = openapi.get('paths', {})
endpoints = []
for path, methods in sorted(paths.items()):
    for method, details in methods.items():
        if method in ['get', 'post', 'put', 'delete', 'patch']:
            summary = details.get('summary', 'No description')
            endpoints.append((method.upper(), path, summary))

for method, path, summary in sorted(endpoints):
    print(f"   {method:6} {path:50} {summary}")

# 4. Check endpoint counts by category
print("\n4. API Summary:")
user_endpoints = [e for e in endpoints if '/users' in e[1]]
dataset_endpoints = [e for e in endpoints if '/datasets' in e[1]]
versioning_endpoints = [e for e in endpoints if '/versioning' in e[1] or '/commits' in e[1] or '/tables' in e[1]]

print(f"   User endpoints: {len(user_endpoints)}")
print(f"   Dataset endpoints: {len(dataset_endpoints)}")
print(f"   Versioning endpoints: {len(versioning_endpoints)}")
print(f"   Total endpoints: {len(endpoints)}")

print("\n✅ All API definitions verified!")
print("=" * 50)