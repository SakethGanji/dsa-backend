#!/usr/bin/env python3
"""Simple test for SQL transform API endpoint."""

import json
import time
import requests

# API configuration
API_BASE = "http://localhost:8000"
API_ENDPOINT = f"{API_BASE}/api/workbench/sql-transform"
TOKEN = "test-token-123"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# Use dataset ID 1 which we know exists
DATASET_ID = 1

print("Testing SQL Transform API")
print("=" * 50)

# Test 1: Basic preview
print("\n1. Testing basic preview mode:")
request = {
    "sources": [{
        "alias": "ev_data",
        "dataset_id": DATASET_ID,
        "ref": "main",
        "table_key": "default"
    }],
    "sql": "SELECT * FROM ev_data",
    "save": False,
    "limit": 5,
    "offset": 0
}

response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
print(f"Status: {response.status_code}")
if response.ok:
    data = response.json()
    print(f"Success! Rows returned: {data.get('row_count', 0)}")
    print(f"Execution time: {data.get('execution_time_ms', 0)}ms")
    print(f"Has more rows: {data.get('has_more', False)}")
    if data.get('columns'):
        print(f"Columns: {[col['name'] for col in data['columns']][:5]}...")
else:
    print(f"Error: {response.json()}")

# Test 2: Preview with SQL transformation
print("\n2. Testing preview with transformation:")
request = {
    "sources": [{
        "alias": "ev",
        "dataset_id": DATASET_ID,
        "ref": "main",
        "table_key": "default"
    }],
    "sql": """
    SELECT 
        logical_row_id,
        data,
        'preview_test' as test_column
    FROM ev 
    LIMIT 3
    """,
    "save": False
}

response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
print(f"Status: {response.status_code}")
if response.ok:
    data = response.json()
    print(f"Success! Rows: {data.get('row_count', 0)}")
    if data.get('data') and len(data['data']) > 0:
        print("First row keys:", list(data['data'][0].keys()))
else:
    print(f"Error: {response.json()}")

# Test 3: Save mode (create job)
print("\n3. Testing save mode (creates async job):")
request = {
    "sources": [{
        "alias": "source",
        "dataset_id": DATASET_ID,
        "ref": "main",
        "table_key": "default"
    }],
    "sql": """
    SELECT 
        logical_row_id,
        data,
        'transformed_on_' || CURRENT_DATE as transform_date
    FROM source
    LIMIT 10
    """,
    "save": True,
    "target": {
        "dataset_id": DATASET_ID,
        "ref": "main",
        "table_key": "api_test_transform",
        "message": "Test transformation via API"
    }
}

response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
print(f"Status: {response.status_code}")
if response.ok:
    data = response.json()
    print(f"Success! Job created:")
    print(f"  Job ID: {data.get('job_id')}")
    print(f"  Status: {data.get('status')}")
    
    # Wait and check job status
    if data.get('job_id'):
        print("\nWaiting 3 seconds for job to complete...")
        time.sleep(3)
        
        job_response = requests.get(
            f"{API_BASE}/api/jobs/{data['job_id']}",
            headers=HEADERS
        )
        if job_response.ok:
            job = job_response.json()
            print(f"Job status: {job.get('status')}")
            if job.get('status') == 'failed':
                print(f"Error: {job.get('error_message')}")
else:
    print(f"Error: {response.json()}")

# Test 4: Error case - missing target
print("\n4. Testing error handling (missing target):")
request = {
    "sources": [{
        "alias": "d",
        "dataset_id": DATASET_ID,
        "ref": "main",
        "table_key": "default"
    }],
    "sql": "SELECT * FROM d",
    "save": True
    # Missing target!
}

response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
print(f"Status: {response.status_code}")
if response.status_code == 422:
    print("Correct! Got validation error for missing target")
else:
    print(f"Unexpected response: {response.text[:200]}")

# Test 5: Pagination test
print("\n5. Testing pagination:")
for offset in [0, 2, 4]:
    request = {
        "sources": [{
            "alias": "ev",
            "dataset_id": DATASET_ID,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT logical_row_id FROM ev",
        "save": False,
        "limit": 2,
        "offset": offset
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    if response.ok:
        data = response.json()
        print(f"  Offset {offset}: {data.get('row_count')} rows")

print("\n" + "=" * 50)
print("Tests completed!")