#!/usr/bin/env python3
"""Test the consolidated SQL transform API endpoint."""

import os
import sys
import json
import time
import requests
from typing import Dict, Any

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# API configuration
API_BASE = "http://localhost:8000"
API_ENDPOINT = f"{API_BASE}/api/workbench/sql-transform"

# Get auth token from environment or use test token
TOKEN = os.getenv("DSA_TOKEN", "test-token-123")
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}


def test_preview_mode():
    """Test preview mode (save=false)."""
    print("\n=== Testing Preview Mode ===")
    
    # Test 1: Basic preview
    print("\n1. Basic preview with pagination:")
    request = {
        "sources": [{
            "alias": "d",
            "dataset_id": 1,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT * FROM d",
        "save": False,
        "limit": 5,
        "offset": 0
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code}")
    if response.ok:
        data = response.json()
        print(f"Rows returned: {data.get('row_count', 0)}")
        print(f"Has more: {data.get('has_more', False)}")
        print(f"Execution time: {data.get('execution_time_ms', 0)}ms")
        if data.get('data'):
            print(f"First row: {json.dumps(data['data'][0], indent=2)}")
    else:
        print(f"Error: {response.text}")
    
    # Test 2: Preview with JOIN
    print("\n2. Preview with JOIN:")
    request = {
        "sources": [
            {
                "alias": "d1",
                "dataset_id": 1,
                "ref": "main",
                "table_key": "default"
            },
            {
                "alias": "d2",
                "dataset_id": 2,
                "ref": "main",
                "table_key": "default"
            }
        ],
        "sql": "SELECT d1.*, d2.name as d2_name FROM d1 LEFT JOIN d2 ON d1.id = d2.id LIMIT 3",
        "save": False,
        "limit": 10,
        "offset": 0
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code}")
    if response.ok:
        data = response.json()
        print(f"Rows returned: {data.get('row_count', 0)}")
        print(f"Columns: {[col['name'] for col in data.get('columns', [])]}")
    else:
        print(f"Error: {response.text}")
    
    # Test 3: Quick preview mode
    print("\n3. Quick preview mode (with sampling):")
    request = {
        "sources": [{
            "alias": "d",
            "dataset_id": 1,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT * FROM d",
        "save": False,
        "limit": 5,
        "offset": 0,
        "quick_preview": True
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code}")
    if response.ok:
        data = response.json()
        print(f"Rows returned: {data.get('row_count', 0)}")
        print("Note: Results are approximate due to sampling")
    else:
        print(f"Error: {response.text}")


def test_save_mode():
    """Test save mode (save=true)."""
    print("\n=== Testing Save Mode ===")
    
    # Test 1: Basic transformation
    print("\n1. Basic transformation:")
    request = {
        "sources": [{
            "alias": "source",
            "dataset_id": 1,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT *, UPPER(name) as name_upper FROM source WHERE id <= 10",
        "save": True,
        "target": {
            "dataset_id": 1,
            "ref": "test-transform",
            "table_key": "transformed",
            "message": "Test transformation - added uppercase names"
        }
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code}")
    if response.ok:
        data = response.json()
        print(f"Job ID: {data.get('job_id')}")
        print(f"Status: {data.get('status')}")
        print(f"Estimated rows: {data.get('estimated_rows')}")
        
        # Check job status
        if data.get('job_id'):
            time.sleep(2)  # Wait a bit
            job_status = check_job_status(data['job_id'])
            print(f"Job final status: {job_status}")
    else:
        print(f"Error: {response.text}")
    
    # Test 2: Transformation with optimistic locking
    print("\n2. Transformation with optimistic locking:")
    
    # First, get current commit ID
    refs_response = requests.get(
        f"{API_BASE}/api/datasets/1/refs",
        headers=HEADERS
    )
    
    if refs_response.ok:
        refs = refs_response.json()
        main_ref = next((r for r in refs if r['name'] == 'main'), None)
        if main_ref:
            current_commit = main_ref['commit_id']
            print(f"Current main commit: {current_commit[:8]}...")
            
            request = {
                "sources": [{
                    "alias": "source",
                    "dataset_id": 1,
                    "ref": "main",
                    "table_key": "default"
                }],
                "sql": "SELECT *, id * 10 as id_times_10 FROM source",
                "save": True,
                "target": {
                    "dataset_id": 1,
                    "ref": "main",
                    "table_key": "with_multiplier",
                    "message": "Added ID multiplier column",
                    "expected_head_commit_id": current_commit
                }
            }
            
            response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
            print(f"Status: {response.status_code}")
            if response.ok:
                data = response.json()
                print(f"Job ID: {data.get('job_id')}")
            else:
                print(f"Error: {response.text}")


def test_error_cases():
    """Test error handling."""
    print("\n=== Testing Error Cases ===")
    
    # Test 1: Missing target when save=true
    print("\n1. Missing target when save=true:")
    request = {
        "sources": [{
            "alias": "d",
            "dataset_id": 1,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT * FROM d",
        "save": True
        # Missing target!
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code}")
    print(f"Error: {response.text}")
    
    # Test 2: Invalid SQL
    print("\n2. Invalid SQL syntax:")
    request = {
        "sources": [{
            "alias": "d",
            "dataset_id": 1,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT * FORM d",  # Typo: FORM instead of FROM
        "save": False
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code}")
    print(f"Error: {response.text}")
    
    # Test 3: Non-existent dataset
    print("\n3. Non-existent dataset:")
    request = {
        "sources": [{
            "alias": "d",
            "dataset_id": 99999,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT * FROM d",
        "save": False
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code}")
    print(f"Error: {response.text}")


def check_job_status(job_id: str) -> str:
    """Check the status of a job."""
    response = requests.get(
        f"{API_BASE}/api/jobs/{job_id}",
        headers=HEADERS
    )
    
    if response.ok:
        job = response.json()
        return job.get('status', 'unknown')
    return 'error'


def main():
    """Run all tests."""
    print("Testing SQL Transform API")
    print("=" * 50)
    
    # Check if server is running
    try:
        health = requests.get(f"{API_BASE}/health")
        if not health.ok:
            print("ERROR: Server is not running on http://localhost:8000")
            print("Please start the server first")
            return
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to server on http://localhost:8000")
        print("Please start the server first")
        return
    
    # Run tests
    test_preview_mode()
    test_save_mode()
    test_error_cases()
    
    print("\n" + "=" * 50)
    print("All tests completed!")


if __name__ == "__main__":
    main()