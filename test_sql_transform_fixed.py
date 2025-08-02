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


def get_test_dataset_info():
    """Get info about available test datasets."""
    print("\n=== Getting Test Dataset Info ===")
    
    # List datasets
    response = requests.get(f"{API_BASE}/api/datasets", headers=HEADERS)
    if response.ok:
        datasets = response.json()
        if datasets:
            # Use first dataset for testing
            dataset = datasets[0]
            dataset_id = dataset['id']
            print(f"Using dataset: {dataset['name']} (ID: {dataset_id})")
            
            # Get refs for this dataset
            refs_response = requests.get(
                f"{API_BASE}/api/datasets/{dataset_id}/refs",
                headers=HEADERS
            )
            if refs_response.ok:
                refs = refs_response.json()
                main_ref = next((r for r in refs if r['name'] == 'main'), None)
                if main_ref:
                    print(f"Main ref commit: {main_ref['commit_id'][:8]}...")
                    return dataset_id, main_ref['commit_id']
    
    print("No datasets found for testing")
    return None, None


def test_preview_mode(dataset_id: int):
    """Test preview mode (save=false)."""
    print("\n=== Testing Preview Mode ===")
    
    # Test 1: Basic preview
    print("\n1. Basic preview with pagination:")
    request = {
        "sources": [{
            "alias": "source_data",
            "dataset_id": dataset_id,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT * FROM source_data",
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
        if data.get('data') and len(data['data']) > 0:
            print(f"Sample data: {json.dumps(data['data'][0], indent=2)}")
    else:
        print(f"Error: {response.text}")
    
    # Test 2: Preview with filtering
    print("\n2. Preview with WHERE clause:")
    request = {
        "sources": [{
            "alias": "src",
            "dataset_id": dataset_id,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT logical_row_id, data FROM src LIMIT 3",
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
    
    # Test 3: Preview with offset
    print("\n3. Preview with pagination offset:")
    request = {
        "sources": [{
            "alias": "src",
            "dataset_id": dataset_id,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT * FROM src",
        "save": False,
        "limit": 2,
        "offset": 2
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code}")
    if response.ok:
        data = response.json()
        print(f"Rows returned: {data.get('row_count', 0)}")
        print(f"With offset {request['offset']}")
    else:
        print(f"Error: {response.text}")


def test_save_mode(dataset_id: int, current_commit: str):
    """Test save mode (save=true)."""
    print("\n=== Testing Save Mode ===")
    
    # Test 1: Basic transformation
    print("\n1. Basic transformation to new table key:")
    request = {
        "sources": [{
            "alias": "source",
            "dataset_id": dataset_id,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT logical_row_id, data, 'transformed' as status FROM source LIMIT 10",
        "save": True,
        "target": {
            "dataset_id": dataset_id,
            "ref": "main",
            "table_key": "test_transformed",
            "message": "Test transformation - added status column"
        }
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code}")
    if response.ok:
        data = response.json()
        print(f"Job ID: {data.get('job_id')}")
        print(f"Status: {data.get('status')}")
        
        # Check job status after a moment
        if data.get('job_id'):
            time.sleep(3)
            job_status = check_job_status(data['job_id'])
            print(f"Job final status: {job_status}")
    else:
        print(f"Error: {response.text}")
    
    # Test 2: Dry run
    print("\n2. Dry run (validation only):")
    request = {
        "sources": [{
            "alias": "source",
            "dataset_id": dataset_id,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT logical_row_id, data FROM source",
        "save": True,
        "dry_run": True,
        "target": {
            "dataset_id": dataset_id,
            "ref": "main",
            "table_key": "dry_run_test",
            "message": "This is a dry run"
        }
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code}")
    if response.ok:
        data = response.json()
        print(f"Job ID: {data.get('job_id')} (should be dry-run indicator)")
        print(f"Status: {data.get('status')}")
    else:
        print(f"Error: {response.text}")


def test_error_cases(dataset_id: int):
    """Test error handling."""
    print("\n=== Testing Error Cases ===")
    
    # Test 1: Missing target when save=true
    print("\n1. Missing target when save=true:")
    request = {
        "sources": [{
            "alias": "d",
            "dataset_id": dataset_id,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT * FROM d",
        "save": True
        # Missing target!
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code} (should be 422)")
    error_msg = response.json().get('detail', [{}])[0].get('msg', '') if response.status_code == 422 else response.text
    print(f"Error: {error_msg}")
    
    # Test 2: Invalid SQL
    print("\n2. Invalid SQL syntax:")
    request = {
        "sources": [{
            "alias": "d",
            "dataset_id": dataset_id,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT * FORM d",  # Typo: FORM instead of FROM
        "save": False
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code}")
    print(f"Error: {response.json().get('message', response.text)}")
    
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
    print(f"Error: {response.json().get('message', response.text)}")
    
    # Test 4: Duplicate aliases
    print("\n4. Duplicate source aliases:")
    request = {
        "sources": [
            {
                "alias": "data",
                "dataset_id": dataset_id,
                "ref": "main",
                "table_key": "default"
            },
            {
                "alias": "data",  # Duplicate!
                "dataset_id": dataset_id,
                "ref": "main",
                "table_key": "default"
            }
        ],
        "sql": "SELECT * FROM data",
        "save": False
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    print(f"Status: {response.status_code} (should be 422)")
    error_msg = response.json().get('detail', [{}])[0].get('msg', '') if response.status_code == 422 else response.text
    print(f"Error: {error_msg}")


def test_performance_features(dataset_id: int):
    """Test performance optimization features."""
    print("\n=== Testing Performance Features ===")
    
    # Test 1: Regular preview vs quick preview
    print("\n1. Comparing regular vs quick preview:")
    
    # Regular preview
    request = {
        "sources": [{
            "alias": "src",
            "dataset_id": dataset_id,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT * FROM src",
        "save": False,
        "limit": 100,
        "quick_preview": False
    }
    
    start = time.time()
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    regular_time = (time.time() - start) * 1000
    
    if response.ok:
        data = response.json()
        print(f"Regular preview: {data.get('row_count')} rows in {regular_time:.0f}ms")
    
    # Quick preview (with sampling) - Note: may fail if table doesn't support TABLESAMPLE
    request['quick_preview'] = True
    request['limit'] = 10
    
    start = time.time()
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    quick_time = (time.time() - start) * 1000
    
    if response.ok:
        data = response.json()
        print(f"Quick preview: {data.get('row_count')} rows in {quick_time:.0f}ms (approximate)")
    else:
        print(f"Quick preview not supported: {response.json().get('message', '')}")
    
    # Test 2: Large limit handling
    print("\n2. Testing large limit handling:")
    request = {
        "sources": [{
            "alias": "src",
            "dataset_id": dataset_id,
            "ref": "main",
            "table_key": "default"
        }],
        "sql": "SELECT * FROM src",
        "save": False,
        "limit": 10000  # Max limit
    }
    
    response = requests.post(API_ENDPOINT, json=request, headers=HEADERS)
    if response.ok:
        data = response.json()
        print(f"Requested limit: {request['limit']}")
        print(f"Actual rows returned: {data.get('row_count', 0)}")
        print(f"Has more: {data.get('has_more', False)}")


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
    
    # Get test dataset info
    dataset_id, current_commit = get_test_dataset_info()
    if not dataset_id:
        print("ERROR: No datasets available for testing")
        print("Please create a dataset first")
        return
    
    # Run tests
    test_preview_mode(dataset_id)
    test_save_mode(dataset_id, current_commit)
    test_error_cases(dataset_id)
    test_performance_features(dataset_id)
    
    print("\n" + "=" * 50)
    print("All tests completed!")


if __name__ == "__main__":
    main()