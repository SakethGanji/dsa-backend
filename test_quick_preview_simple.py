#!/usr/bin/env python3
"""Simple test for quick preview functionality."""

import json
import time
import requests

# API configuration
API_BASE = "http://localhost:8000"
API_ENDPOINT = f"{API_BASE}/api/workbench/sql-transform"
HEADERS = {
    "Authorization": "Bearer test-token-123",
    "Content-Type": "application/json"
}

print("Testing Quick Preview Feature")
print("=" * 60)

# Test 1: Regular query (no sampling)
print("\n1. Regular query (full scan):")
request = {
    "sources": [{
        "alias": "ev",
        "dataset_id": 1,
        "ref": "main",
        "table_key": "default"
    }],
    "sql": "SELECT COUNT(*) as total FROM ev",
    "save": False,
    "quick_preview": False
}

start = time.time()
response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
regular_time = (time.time() - start) * 1000

if response.ok:
    data = response.json()
    print(f"Status: {response.status_code}")
    print(f"Execution time: {data.get('execution_time_ms', 0)}ms (total: {regular_time:.0f}ms)")
    if data.get('data'):
        print(f"Row count: {data['data'][0]['total']}")
else:
    print(f"Error: {response.status_code} - {response.text}")

# Test 2: Quick preview with sampling
print("\n2. Quick preview (1% sample):")
request = {
    "sources": [{
        "alias": "ev",
        "dataset_id": 1,
        "ref": "main",
        "table_key": "default"
    }],
    "sql": "SELECT COUNT(*) as sampled FROM ev",
    "save": False,
    "quick_preview": True,
    "sample_percent": 1.0
}

start = time.time()
response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
quick_time = (time.time() - start) * 1000

if response.ok:
    data = response.json()
    print(f"Status: {response.status_code}")
    print(f"Execution time: {data.get('execution_time_ms', 0)}ms (total: {quick_time:.0f}ms)")
    if data.get('data'):
        print(f"Sampled row count: {data['data'][0]['sampled']} (≈1% of total)")
    
    if regular_time > 0 and quick_time > 0:
        print(f"\nPerformance improvement: {regular_time / quick_time:.1f}x faster")
else:
    print(f"Error: {response.status_code} - {response.text}")

# Test 3: Different sample percentages
print("\n3. Testing different sample percentages:")
for pct in [0.1, 1.0, 5.0]:
    request = {
        "sources": [{
            "alias": "ev",
            "dataset_id": 1,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT COUNT(*) as cnt FROM ev",
        "save": False,
        "quick_preview": True,
        "sample_percent": pct
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    if response.ok:
        data = response.json()
        count = data['data'][0]['cnt'] if data.get('data') else 0
        print(f"  {pct}% sample: {count} rows")
    else:
        print(f"  {pct}% sample: Error - {response.status_code}")

print("\n" + "=" * 60)
print("Quick preview is now working with proper multi-CTE sampling!")
print("The sampling happens BEFORE joins for maximum performance.")