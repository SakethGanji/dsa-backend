#!/usr/bin/env python3
"""
E2E API Test Suite for DSA Platform
Tests all API endpoints including table analysis and multi-sheet Excel support
"""

import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import sys
from typing import Dict, Any, Optional
import os

# Configuration
BASE_URL = "http://localhost:8000"
USER_SOEID = "bg54677"  # 7-character SOEID
USER_PASSWORD = "string"
USER_NAME = "Test User"

# Test results tracking
test_results = {"passed": 0, "failed": 0, "total": 0}

# Colors for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

def print_phase(phase_name: str):
    """Print phase header"""
    print(f"\n{YELLOW}{'='*60}{RESET}")
    print(f"{YELLOW}{phase_name}{RESET}")
    print(f"{YELLOW}{'='*60}{RESET}")

def print_test_result(test_name: str, passed: bool, details: str = ""):
    """Print test result with color coding"""
    test_results["total"] += 1
    if passed:
        test_results["passed"] += 1
        print(f"{GREEN}✓ {test_name}{RESET}")
    else:
        test_results["failed"] += 1
        print(f"{RED}✗ {test_name}{RESET}")
        if details:
            print(f"  {RED}Details: {details}{RESET}")

def test_endpoint(method: str, endpoint: str, expected_status: int, 
                 headers: Optional[Dict] = None, json_data: Optional[Dict] = None,
                 files: Optional[Dict] = None, params: Optional[Dict] = None) -> Dict[str, Any]:
    """Test an API endpoint and return response data"""
    url = f"{BASE_URL}{endpoint}"
    
    try:
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=json_data,
            files=files,
            params=params
        )
        
        success = response.status_code == expected_status
        test_name = f"{method} {endpoint} (Expected: {expected_status}, Got: {response.status_code})"
        
        print_test_result(test_name, success, 
                         f"Response: {response.text[:200]}..." if not success else "")
        
        # Return response data
        return {
            "success": success,
            "status_code": response.status_code,
            "data": response.json() if response.text else None,
            "headers": dict(response.headers)
        }
        
    except Exception as e:
        print_test_result(f"{method} {endpoint}", False, str(e))
        return {"success": False, "error": str(e)}

def create_multi_sheet_excel(filepath: str):
    """Create a multi-sheet Excel file for testing"""
    print(f"\n{BLUE}Creating multi-sheet Excel test file...{RESET}")
    
    # Set random seed for reproducibility
    np.random.seed(42)
    
    # Sheet 1: Sales Data
    dates = pd.date_range(start='2024-01-01', end='2024-03-31', freq='D')
    sales_data = {
        'Date': dates,
        'Product': np.random.choice(['Laptop', 'Phone', 'Tablet', 'Monitor'], len(dates)),
        'Region': np.random.choice(['North', 'South', 'East', 'West'], len(dates)),
        'Sales': np.random.randint(100, 5000, len(dates)),
        'Quantity': np.random.randint(1, 50, len(dates)),
        'Discount': np.random.uniform(0, 0.3, len(dates))
    }
    df_sales = pd.DataFrame(sales_data)
    
    # Sheet 2: Customer Data
    customer_data = {
        'CustomerID': range(1, 101),
        'Name': [f'Customer_{i}' for i in range(1, 101)],
        'Email': [f'customer{i}@example.com' for i in range(1, 101)],
        'RegisterDate': pd.date_range(start='2023-01-01', periods=100, freq='D'),
        'TotalPurchases': np.random.randint(0, 50000, 100),
        'Category': np.random.choice(['Premium', 'Standard', 'Basic'], 100),
        'Active': np.random.choice([True, False], 100, p=[0.8, 0.2])
    }
    df_customers = pd.DataFrame(customer_data)
    
    # Sheet 3: Product Inventory
    product_data = {
        'ProductID': range(1, 51),
        'ProductName': [f'Product_{i}' for i in range(1, 51)],
        'Category': np.random.choice(['Electronics', 'Accessories', 'Software'], 50),
        'Stock': np.random.randint(0, 1000, 50),
        'Price': np.random.uniform(50, 5000, 50).round(2),
        'LastRestocked': pd.date_range(start='2024-01-01', periods=50, freq='W'),
        'SupplierID': np.random.randint(1, 10, 50)
    }
    df_products = pd.DataFrame(product_data)
    
    # Add some null values to test null handling
    df_sales.loc[np.random.choice(df_sales.index, 10), 'Discount'] = None
    df_customers.loc[np.random.choice(df_customers.index, 5), 'Email'] = None
    df_products.loc[np.random.choice(df_products.index, 7), 'LastRestocked'] = None
    
    # Write to Excel file
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        df_sales.to_excel(writer, sheet_name='Sales', index=False)
        df_customers.to_excel(writer, sheet_name='Customers', index=False)
        df_products.to_excel(writer, sheet_name='Products', index=False)
    
    print(f"{GREEN}✓ Multi-sheet Excel file created at {filepath}{RESET}")
    return filepath

def main():
    """Run the complete E2E test suite"""
    print(f"{BLUE}=== DSA Platform E2E API Test Suite ==={RESET}")
    print(f"Base URL: {BASE_URL}")
    print(f"Test User SOEID: {USER_SOEID}")
    
    # Variables to store test data
    token = None
    user_id = None
    dataset_id = None
    table_keys = []
    commit_id = None
    job_id = None
    
    # Phase 0: Sanity & Health Checks
    print_phase("Phase 0: Sanity & Health Checks")
    
    # Test health endpoint
    test_endpoint("GET", "/health", 200)
    
    # Test root endpoint
    test_endpoint("GET", "/", 200)
    
    # Phase 1: User Authentication & Authorization
    print_phase("Phase 1: User Authentication & Authorization")
    
    # Register user using public endpoint (admin-only endpoint would require auth)
    # First try to register, if it fails with 400 (already exists), that's OK
    result = test_endpoint("POST", "/api/users/register-public", 201, 
                          json_data={
                              "soeid": USER_SOEID,
                              "password": USER_PASSWORD,
                              "role_id": 1  # Assuming 1 is admin role
                          })
    
    # If registration failed because user exists, that's OK for testing
    if result["status_code"] == 400:
        print(f"  {YELLOW}User already exists, continuing with login{RESET}")
    
    if result["success"] and result["data"]:
        user_id = result["data"].get("id")
        print(f"  User ID: {user_id}")
    
    # Login using OAuth2 form data
    # The login endpoint expects form data, not JSON
    import urllib.parse
    form_data = urllib.parse.urlencode({
        "username": USER_SOEID,  # OAuth2 expects "username" field
        "password": USER_PASSWORD
    })
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/users/login",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=form_data
        )
        
        result = {
            "success": response.status_code == 200,
            "status_code": response.status_code,
            "data": response.json() if response.text else None
        }
        
        print_test_result(f"POST /api/users/login (Expected: 200, Got: {response.status_code})", 
                         result["success"],
                         f"Response: {response.text[:200]}..." if not result["success"] else "")
    except Exception as e:
        result = {"success": False, "error": str(e)}
        print_test_result("POST /api/users/login", False, str(e))
    
    if result["success"] and result["data"]:
        token = result["data"].get("access_token")
        print(f"  Token obtained: {token[:20]}...")
    else:
        print(f"{RED}Failed to obtain auth token. Exiting.{RESET}")
        sys.exit(1)
    
    # Create auth headers for subsequent requests
    auth_headers = {"Authorization": f"Bearer {token}"}
    
    # Test duplicate registration (should fail) - only if we successfully created user
    if result["status_code"] == 201:
        test_endpoint("POST", "/api/users/register-public", 400,
                     json_data={
                         "soeid": USER_SOEID,
                         "password": USER_PASSWORD
                     })
    
    # Test login with wrong password
    form_data_wrong = urllib.parse.urlencode({
        "username": USER_SOEID,
        "password": "wrongpassword"
    })
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/users/login",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=form_data_wrong
        )
        
        success = response.status_code == 401
        print_test_result(f"POST /api/users/login (wrong password) (Expected: 401, Got: {response.status_code})", 
                         success,
                         f"Response: {response.text[:200]}..." if not success else "")
    except Exception as e:
        print_test_result("POST /api/users/login (wrong password)", False, str(e))
    
    # Test protected endpoint without auth
    test_endpoint("GET", "/api/datasets/", 401)
    
    # Phase 2: Core Resource Lifecycle (Datasets)
    print_phase("Phase 2: Core Resource Lifecycle (Datasets)")
    
    # Create dataset - API returns 200, not 201
    # Add timestamp to make dataset name unique
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_name = f"Test Dataset E2E {timestamp}"
    
    result = test_endpoint("POST", "/api/datasets/", 200,
                          headers=auth_headers,
                          json_data={
                              "name": dataset_name,
                              "description": "Dataset for E2E testing"
                          })
    
    if result["success"] and result["data"]:
        dataset_id = result["data"].get("dataset_id") or result["data"].get("id")
        print(f"  Dataset ID: {dataset_id}")
    else:
        print(f"{RED}Failed to create dataset. Exiting.{RESET}")
        sys.exit(1)
    
    # Get dataset
    test_endpoint("GET", f"/api/datasets/{dataset_id}", 200, headers=auth_headers)
    
    # List datasets
    test_endpoint("GET", "/api/datasets/", 200, headers=auth_headers)
    
    # Update dataset
    test_endpoint("PATCH", f"/api/datasets/{dataset_id}", 200,
                 headers=auth_headers,
                 json_data={
                     "description": "Updated description for E2E testing"
                 })
    
    # Get dataset overview
    test_endpoint("GET", f"/api/datasets/{dataset_id}/overview", 200, headers=auth_headers)
    
    # Phase 3: Prepare test data
    print_phase("Phase 3: Preparing Test Data")
    
    excel_file = "/tmp/multi_sheet_test.xlsx"
    create_multi_sheet_excel(excel_file)
    
    # Phase 4: Data Versioning & History
    print_phase("Phase 4: Data Versioning & History")
    
    # List refs
    test_endpoint("GET", f"/api/datasets/{dataset_id}/refs", 200, headers=auth_headers)
    
    # Import multi-sheet Excel file
    print(f"\n{BLUE}Importing multi-sheet Excel file...{RESET}")
    with open(excel_file, 'rb') as f:
        # Create multipart form data with both file and commit_message
        files = {
            'file': ('multi_sheet_test.xlsx', f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        }
        data = {
            'commit_message': 'Initial import of multi-sheet Excel data'
        }
        
        try:
            response = requests.post(
                f"{BASE_URL}/api/datasets/{dataset_id}/refs/main/import",
                headers=auth_headers,
                files=files,
                data=data  # Form data, not JSON
            )
            
            result = {
                "success": response.status_code == 200,
                "status_code": response.status_code,
                "data": response.json() if response.text else None
            }
            
            print_test_result(f"POST /api/datasets/{dataset_id}/refs/main/import (Expected: 200, Got: {response.status_code})", 
                             result["success"],
                             f"Response: {response.text[:200]}..." if not result["success"] else "")
        except Exception as e:
            result = {"success": False, "error": str(e)}
            print_test_result(f"POST /api/datasets/{dataset_id}/refs/main/import", False, str(e))
    
    # Wait for import to complete
    print(f"{BLUE}Waiting for import to complete...{RESET}")
    time.sleep(5)
    
    # Get data from main branch
    test_endpoint("GET", f"/api/datasets/{dataset_id}/refs/main/data", 200, headers=auth_headers)
    
    # List tables
    result = test_endpoint("GET", f"/api/datasets/{dataset_id}/refs/main/tables", 200, headers=auth_headers)
    
    if result["success"] and result["data"]:
        # Handle both list and dict response structures
        if isinstance(result["data"], list):
            # If it's a list of strings
            if result["data"] and isinstance(result["data"][0], str):
                table_keys = result["data"]
            else:
                # If it's a list of objects
                table_keys = [table.get("key", table.get("table_key", "")) for table in result["data"]]
        elif isinstance(result["data"], dict) and "tables" in result["data"]:
            # If it's a dict with tables key containing strings
            if result["data"]["tables"] and isinstance(result["data"]["tables"][0], str):
                table_keys = result["data"]["tables"]
            else:
                # If it's a dict with tables key containing objects
                table_keys = [table.get("key", table.get("table_key", "")) for table in result["data"]["tables"]]
        else:
            table_keys = []
        
        print(f"  Found {len(table_keys)} tables: {', '.join(table_keys)}")
    
    if not table_keys:
        print(f"{RED}No tables found after import. Exiting.{RESET}")
        sys.exit(1)
    
    # Test table endpoints for first table
    table_key = table_keys[0]
    print(f"\n{BLUE}Testing table: {table_key}{RESET}")
    
    # Get table data
    test_endpoint("GET", f"/api/datasets/{dataset_id}/refs/main/tables/{table_key}/data", 200,
                 headers=auth_headers, params={"limit": 10})
    
    # Get table schema
    test_endpoint("GET", f"/api/datasets/{dataset_id}/refs/main/tables/{table_key}/schema", 200,
                 headers=auth_headers)
    
    # Get table analysis (KEY ENDPOINT)
    print(f"\n{YELLOW}Testing Table Analysis Endpoint{RESET}")
    result = test_endpoint("GET", f"/api/datasets/{dataset_id}/refs/main/tables/{table_key}/analysis", 200,
                          headers=auth_headers)
    
    if result["success"] and result["data"]:
        print(f"\n{BLUE}Table Analysis Response:{RESET}")
        analysis_data = result["data"]
        print(f"  Table Key: {analysis_data.get('table_key')}")
        print(f"  Total Rows: {analysis_data.get('total_rows')}")
        print(f"  Columns: {', '.join(analysis_data.get('columns', [])[:5])}...")
        print(f"  Column Types: {json.dumps(dict(list(analysis_data.get('column_types', {}).items())[:3]), indent=2)}")
        print(f"  Null Counts: {json.dumps(dict(list(analysis_data.get('null_counts', {}).items())[:3]), indent=2)}")
        print(f"  Sample Values: {len(analysis_data.get('sample_values', {}))} columns with samples")
    
    # Test analysis for all tables (multi-sheet validation)
    print(f"\n{YELLOW}Testing Analysis for All Tables (Multi-sheet Excel){RESET}")
    for table in table_keys:
        result = test_endpoint("GET", f"/api/datasets/{dataset_id}/refs/main/tables/{table}/analysis", 200,
                             headers=auth_headers)
        if result["success"] and result["data"]:
            print(f"  {GREEN}✓ Table '{table}' - {result['data'].get('total_rows')} rows, {len(result['data'].get('columns', []))} columns{RESET}")
    
    # Create new branch - check correct field name
    test_endpoint("POST", f"/api/datasets/{dataset_id}/refs", 200,
                 headers=auth_headers,
                 json_data={
                     "name": "feature-branch",
                     "from_ref": "main"  # Field is from_ref, not source
                 })
    
    # Get commit history
    result = test_endpoint("GET", f"/api/datasets/{dataset_id}/history", 200, headers=auth_headers)
    
    if result["success"] and result["data"]:
        # Handle different response structures
        commits = result["data"]
        if isinstance(result["data"], dict) and "commits" in result["data"]:
            commits = result["data"]["commits"]
        
        if commits and len(commits) > 0:
            commit_id = commits[0].get("id", commits[0].get("commit_id"))
            print(f"  Latest commit ID: {commit_id}")
        
        # Get commit schema
        test_endpoint("GET", f"/api/datasets/{dataset_id}/commits/{commit_id}/schema", 200,
                     headers=auth_headers)
        
        # Get commit data
        test_endpoint("GET", f"/api/datasets/{dataset_id}/commits/{commit_id}/data", 200,
                     headers=auth_headers)
    
    # Delete branch - might return 200 or 204
    test_endpoint("DELETE", f"/api/datasets/{dataset_id}/refs/feature-branch", 200,
                 headers=auth_headers)
    
    # Phase 5: Search & Discovery
    print_phase("Phase 5: Search & Discovery")
    
    # Search datasets
    test_endpoint("GET", "/api/datasets/search/", 200,
                 headers=auth_headers,
                 params={"q": "E2E"})
    
    # Search non-existent
    test_endpoint("GET", "/api/datasets/search/", 200,
                 headers=auth_headers,
                 params={"q": "nonexistentterm123"})
    
    # Search suggestions
    test_endpoint("GET", "/api/datasets/search/suggest", 200,
                 headers=auth_headers,
                 params={"q": "Tes"})
    
    # Phase 6: Asynchronous Operations (Jobs & Sampling)
    print_phase("Phase 6: Asynchronous Operations (Jobs & Sampling)")
    
    # Get sampling methods
    test_endpoint("GET", f"/api/sampling/datasets/{dataset_id}/sampling-methods", 200,
                 headers=auth_headers)
    
    # Create sampling job
    result = test_endpoint("POST", f"/api/sampling/datasets/{dataset_id}/jobs", 202,
                          headers=auth_headers,
                          json_data={
                              "method": "random",
                              "sample_size": 10,
                              "parameters": {}
                          })
    
    if result["success"] and result["data"]:
        job_id = result["data"].get("id")
        print(f"  Sampling job ID: {job_id}")
        
        # Poll for job completion
        print(f"{BLUE}Polling for job completion...{RESET}")
        for i in range(30):
            result = test_endpoint("GET", f"/api/jobs/{job_id}", 200, headers=auth_headers)
            if result["success"] and result["data"]:
                status = result["data"].get("status")
                if status == "completed":
                    print(f"  {GREEN}Job completed!{RESET}")
                    break
                elif status == "failed":
                    print(f"  {RED}Job failed!{RESET}")
                    break
            time.sleep(1)
        
        # Get sampling job data
        test_endpoint("GET", f"/api/sampling/jobs/{job_id}/data", 200, headers=auth_headers)
        
        # Get residual data
        test_endpoint("GET", f"/api/sampling/jobs/{job_id}/residual", 200, headers=auth_headers)
    
    # List all jobs
    test_endpoint("GET", "/api/jobs", 200, headers=auth_headers)
    
    # Get sampling history for dataset
    test_endpoint("GET", f"/api/sampling/datasets/{dataset_id}/history", 200, headers=auth_headers)
    
    # Get sampling history for user
    if user_id:
        test_endpoint("GET", f"/api/sampling/users/{user_id}/history", 200, headers=auth_headers)
    
    # Direct sampling (synchronous)
    test_endpoint("POST", f"/api/sampling/datasets/{dataset_id}/sample", 200,
                 headers=auth_headers,
                 json_data={
                     "method": "random",
                     "sample_size": 5,
                     "parameters": {}
                 })
    
    # Column sampling
    test_endpoint("POST", f"/api/sampling/datasets/{dataset_id}/column-samples", 200,
                 headers=auth_headers,
                 json_data={
                     "columns": ["column1", "column2"],
                     "sample_size": 5
                 })
    
    # Phase 7: Cleanup
    print_phase("Phase 7: Cleanup")
    
    # Delete dataset - might return 200 or 204
    test_endpoint("DELETE", f"/api/datasets/{dataset_id}", 200, headers=auth_headers)
    
    # Verify dataset is deleted
    test_endpoint("GET", f"/api/datasets/{dataset_id}", 404, headers=auth_headers)
    
    # Clean up test file
    if os.path.exists(excel_file):
        os.remove(excel_file)
        print(f"{GREEN}✓ Cleaned up test file{RESET}")
    
    # Print summary
    print_phase("Test Summary")
    print(f"Total Tests: {test_results['total']}")
    print(f"{GREEN}Passed: {test_results['passed']}{RESET}")
    print(f"{RED}Failed: {test_results['failed']}{RESET}")
    
    if test_results['failed'] == 0:
        print(f"\n{GREEN}✓ All tests passed!{RESET}")
        return 0
    else:
        print(f"\n{RED}✗ Some tests failed!{RESET}")
        return 1

if __name__ == "__main__":
    sys.exit(main())