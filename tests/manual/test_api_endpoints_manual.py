#!/usr/bin/env python3
"""
Comprehensive API endpoint testing script for DSA Platform.
Tests all endpoints including the new table-related functionality.
"""

import asyncio
import httpx
import json
from datetime import datetime
import sys

# API configuration
BASE_URL = "http://localhost:8000"
API_PREFIX = "/api"

# Test data
TEST_USER = {
    "soeid": "testuser1",
    "password": "TestPass123!",
    "role_id": 1
}

TEST_DATASET = {
    "name": f"Test Dataset {datetime.now().isoformat()}",
    "description": "Test dataset for API testing"
}


class APITester:
    def __init__(self):
        self.client = httpx.AsyncClient(base_url=BASE_URL, timeout=30.0)
        self.access_token = None
        self.dataset_id = None
        self.results = []
        
    async def close(self):
        await self.client.aclose()
        
    def log_result(self, endpoint: str, method: str, status: int, success: bool, message: str = ""):
        result = {
            "endpoint": endpoint,
            "method": method,
            "status": status,
            "success": success,
            "message": message
        }
        self.results.append(result)
        
        # Print result
        icon = "✅" if success else "❌"
        print(f"{icon} {method} {endpoint} - Status: {status} {message}")
    
    async def test_health_endpoints(self):
        """Test health check endpoints."""
        print("\n🏥 Testing Health Endpoints...")
        
        # Test health check
        response = await self.client.get("/health")
        self.log_result("/health", "GET", response.status_code, response.status_code == 200)
        
        # Test root endpoint
        response = await self.client.get("/")
        self.log_result("/", "GET", response.status_code, response.status_code == 200)
    
    async def test_user_endpoints(self):
        """Test user authentication endpoints."""
        print("\n👤 Testing User Endpoints...")
        
        # Test public registration (if available)
        response = await self.client.post(
            f"{API_PREFIX}/register-public",
            json={
                "soeid": TEST_USER["soeid"],
                "password": TEST_USER["password"],
                "role_id": TEST_USER["role_id"]
            }
        )
        
        if response.status_code == 200:
            self.log_result("/api/register-public", "POST", response.status_code, True, "User created")
        elif response.status_code == 409:
            self.log_result("/api/register-public", "POST", response.status_code, True, "User already exists")
        else:
            self.log_result("/api/register-public", "POST", response.status_code, False, response.text)
        
        # Test login
        response = await self.client.post(
            f"{API_PREFIX}/login",
            data={
                "username": TEST_USER["soeid"],
                "password": TEST_USER["password"]
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            self.access_token = data["access_token"]
            self.log_result("/api/login", "POST", response.status_code, True, "Login successful")
        else:
            self.log_result("/api/login", "POST", response.status_code, False, "Login failed")
            return False
        
        return True
    
    async def test_dataset_endpoints(self):
        """Test dataset management endpoints."""
        print("\n📊 Testing Dataset Endpoints...")
        
        if not self.access_token:
            print("❌ No access token, skipping authenticated endpoints")
            return
        
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        # Create dataset
        response = await self.client.post(
            f"{API_PREFIX}/datasets",
            json=TEST_DATASET,
            headers=headers
        )
        
        if response.status_code == 200:
            data = response.json()
            self.dataset_id = data["dataset_id"]
            self.log_result("/api/datasets", "POST", response.status_code, True, f"Created dataset {self.dataset_id}")
        else:
            self.log_result("/api/datasets", "POST", response.status_code, False, response.text)
            return
        
        # Get dataset
        response = await self.client.get(
            f"{API_PREFIX}/datasets/{self.dataset_id}",
            headers=headers
        )
        self.log_result(f"/api/datasets/{self.dataset_id}", "GET", response.status_code, response.status_code == 200)
        
        # List user datasets
        response = await self.client.get(
            f"{API_PREFIX}/datasets",
            headers=headers
        )
        self.log_result("/api/datasets", "GET", response.status_code, response.status_code == 200)
        
        # Grant permission (to self - should work as admin)
        response = await self.client.post(
            f"{API_PREFIX}/datasets/{self.dataset_id}/permissions",
            json={
                "user_id": 1,  # Assuming user ID 1
                "permission_type": "write"
            },
            headers=headers
        )
        self.log_result(f"/api/datasets/{self.dataset_id}/permissions", "POST", response.status_code, 
                       response.status_code in [200, 204])
    
    async def test_table_endpoints(self):
        """Test table-specific endpoints."""
        print("\n📋 Testing Table Endpoints...")
        
        if not self.access_token or not self.dataset_id:
            print("❌ No access token or dataset, skipping table endpoints")
            return
        
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        # First, let's create some test data by uploading a file
        # For now, we'll test with an empty dataset
        
        # List tables (should be empty for new dataset)
        response = await self.client.get(
            f"{API_PREFIX}/datasets/{self.dataset_id}/refs/main/tables",
            headers=headers
        )
        
        if response.status_code == 200:
            data = response.json()
            self.log_result(f"/api/datasets/{self.dataset_id}/refs/main/tables", "GET", 
                          response.status_code, True, f"Tables: {data.get('tables', [])}")
        else:
            self.log_result(f"/api/datasets/{self.dataset_id}/refs/main/tables", "GET", 
                          response.status_code, False, response.text)
        
        # Try to get data from a non-existent table (should fail gracefully)
        response = await self.client.get(
            f"{API_PREFIX}/datasets/{self.dataset_id}/refs/main/tables/primary/data",
            headers=headers
        )
        
        # This might return 404 or empty data
        success = response.status_code in [200, 404]
        self.log_result(f"/api/datasets/{self.dataset_id}/refs/main/tables/primary/data", "GET", 
                       response.status_code, success)
        
        # Try to get schema for a non-existent table
        response = await self.client.get(
            f"{API_PREFIX}/datasets/{self.dataset_id}/refs/main/tables/primary/schema",
            headers=headers
        )
        
        success = response.status_code in [200, 404]
        self.log_result(f"/api/datasets/{self.dataset_id}/refs/main/tables/primary/schema", "GET", 
                       response.status_code, success)
    
    async def test_versioning_endpoints(self):
        """Test versioning endpoints."""
        print("\n🔄 Testing Versioning Endpoints...")
        
        if not self.access_token or not self.dataset_id:
            print("❌ No access token or dataset, skipping versioning endpoints")
            return
        
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        # Get data at ref (should be empty)
        response = await self.client.get(
            f"{API_PREFIX}/datasets/{self.dataset_id}/refs/main/data",
            headers=headers
        )
        
        success = response.status_code in [200, 404]
        self.log_result(f"/api/datasets/{self.dataset_id}/refs/main/data", "GET", 
                       response.status_code, success)
        
        # Create a commit with test data
        test_data = [
            {"id": 1, "name": "Test Row 1", "value": 100},
            {"id": 2, "name": "Test Row 2", "value": 200}
        ]
        
        response = await self.client.post(
            f"{API_PREFIX}/datasets/{self.dataset_id}/refs/main/commits",
            json={
                "message": "Test commit",
                "data": test_data
            },
            headers=headers
        )
        
        if response.status_code == 200:
            data = response.json()
            commit_id = data.get("commit_id")
            self.log_result(f"/api/datasets/{self.dataset_id}/refs/main/commits", "POST", 
                          response.status_code, True, f"Created commit {commit_id}")
            
            # Now test table endpoints with data
            await self.test_table_endpoints_with_data()
        else:
            self.log_result(f"/api/datasets/{self.dataset_id}/refs/main/commits", "POST", 
                          response.status_code, False, response.text)
    
    async def test_table_endpoints_with_data(self):
        """Test table endpoints after creating data."""
        print("\n📋 Testing Table Endpoints with Data...")
        
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        # List tables (should now have 'primary')
        response = await self.client.get(
            f"{API_PREFIX}/datasets/{self.dataset_id}/refs/main/tables",
            headers=headers
        )
        
        if response.status_code == 200:
            data = response.json()
            tables = data.get('tables', [])
            self.log_result(f"/api/datasets/{self.dataset_id}/refs/main/tables", "GET", 
                          response.status_code, True, f"Tables: {tables}")
            
            if 'primary' in tables:
                # Get table data
                response = await self.client.get(
                    f"{API_PREFIX}/datasets/{self.dataset_id}/refs/main/tables/primary/data?limit=10",
                    headers=headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    row_count = len(data.get('data', []))
                    self.log_result(f"/api/datasets/{self.dataset_id}/refs/main/tables/primary/data", "GET", 
                                  response.status_code, True, f"Retrieved {row_count} rows")
                else:
                    self.log_result(f"/api/datasets/{self.dataset_id}/refs/main/tables/primary/data", "GET", 
                                  response.status_code, False, response.text)
                
                # Get table schema
                response = await self.client.get(
                    f"{API_PREFIX}/datasets/{self.dataset_id}/refs/main/tables/primary/schema",
                    headers=headers
                )
                
                self.log_result(f"/api/datasets/{self.dataset_id}/refs/main/tables/primary/schema", "GET", 
                              response.status_code, response.status_code == 200)
    
    def print_summary(self):
        """Print test summary."""
        print("\n" + "="*60)
        print("📊 TEST SUMMARY")
        print("="*60)
        
        total = len(self.results)
        passed = sum(1 for r in self.results if r["success"])
        failed = total - passed
        
        print(f"\nTotal Tests: {total}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        
        if failed > 0:
            print("\n❌ Failed Tests:")
            for result in self.results:
                if not result["success"]:
                    print(f"  - {result['method']} {result['endpoint']} (Status: {result['status']}) {result['message']}")
        
        print("\n" + "="*60)
        return failed == 0
    
    async def run_all_tests(self):
        """Run all API tests."""
        print("🚀 Starting DSA Platform API Tests")
        print("="*60)
        
        try:
            # Test endpoints in order
            await self.test_health_endpoints()
            
            if await self.test_user_endpoints():
                await self.test_dataset_endpoints()
                await self.test_versioning_endpoints()
                await self.test_table_endpoints()
            
            # Print summary
            success = self.print_summary()
            return success
            
        except Exception as e:
            print(f"\n❌ Test execution failed: {e}")
            return False
        finally:
            await self.close()


async def main():
    """Main test runner."""
    tester = APITester()
    success = await tester.run_all_tests()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    print("⚠️  Make sure the DSA Platform API is running on http://localhost:8000")
    print("⚠️  Make sure the database is initialized with the schema")
    input("Press Enter to continue...")
    
    asyncio.run(main())