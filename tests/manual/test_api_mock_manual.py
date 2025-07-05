#!/usr/bin/env python3
"""
Mock API testing to verify endpoint definitions and handlers.
This tests the API structure without requiring a running server.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch
import sys

# Import our handlers and endpoints
sys.path.insert(0, '/home/saketh/Projects/dsa/src')

try:
    # Try importing from the src structure
    from src.api import users, datasets, versioning
    from src.features.versioning.get_table_data import (
        GetTableDataHandler, ListTablesHandler, GetTableSchemaHandler
    )
    from src.core.abstractions import IUnitOfWork, ITableReader
except ImportError:
    # If that fails, try direct imports
    import api.users as users
    import api.datasets as datasets
    import api.versioning as versioning
    from features.versioning.get_table_data import (
        GetTableDataHandler, ListTablesHandler, GetTableSchemaHandler
    )
    from core.services.interfaces import IUnitOfWork, ITableReader


class MockAPITester:
    def __init__(self):
        self.results = []
        
    def log_result(self, test_name: str, success: bool, message: str = ""):
        icon = "✅" if success else "❌"
        print(f"{icon} {test_name}: {message}")
        self.results.append((test_name, success))
    
    async def test_table_handlers(self):
        """Test table-specific handlers directly."""
        print("\n📋 Testing Table Handlers...")
        
        # Mock dependencies
        mock_uow = AsyncMock(spec=IUnitOfWork)
        mock_table_reader = AsyncMock(spec=ITableReader)
        
        # Configure mocks
        mock_uow.__aenter__.return_value = mock_uow
        mock_uow.datasets.user_has_permission.return_value = True
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'test123'
        }
        
        # Test ListTablesHandler
        try:
            mock_table_reader.list_table_keys.return_value = ['primary']
            handler = ListTablesHandler(mock_uow, mock_table_reader)
            result = await handler.handle(1, 'main', 1)
            assert result == {'tables': ['primary']}
            self.log_result("ListTablesHandler", True, "Successfully lists tables")
        except Exception as e:
            self.log_result("ListTablesHandler", False, str(e))
        
        # Test GetTableDataHandler
        try:
            mock_table_reader.count_table_rows.return_value = 2
            mock_table_reader.get_table_data.return_value = [
                {'_row_index': 0, 'id': 1, 'name': 'Test 1'},
                {'_row_index': 1, 'id': 2, 'name': 'Test 2'}
            ]
            handler = GetTableDataHandler(mock_uow, mock_table_reader)
            result = await handler.handle(1, 'main', 'primary', 1, 0, 10)
            assert result['table_key'] == 'primary'
            assert result['total_count'] == 2
            assert len(result['data']) == 2
            self.log_result("GetTableDataHandler", True, "Successfully retrieves table data")
        except Exception as e:
            self.log_result("GetTableDataHandler", False, str(e))
        
        # Test GetTableSchemaHandler
        try:
            mock_table_reader.get_table_schema.return_value = {
                'columns': {
                    'id': {'type': 'integer'},
                    'name': {'type': 'string'}
                }
            }
            handler = GetTableSchemaHandler(mock_uow, mock_table_reader)
            result = await handler.handle(1, 'main', 'primary', 1)
            assert result['table_key'] == 'primary'
            assert 'id' in result['schema']['columns']
            self.log_result("GetTableSchemaHandler", True, "Successfully retrieves table schema")
        except Exception as e:
            self.log_result("GetTableSchemaHandler", False, str(e))
    
    async def test_endpoint_definitions(self):
        """Test that endpoints are properly defined."""
        print("\n🔌 Testing Endpoint Definitions...")
        
        # Check user endpoints
        user_routes = [route.path for route in users.router.routes]
        expected_user_routes = ['/login', '/register', '/register-public']
        
        for route in expected_user_routes:
            if any(route in r for r in user_routes):
                self.log_result(f"User endpoint {route}", True, "Defined")
            else:
                self.log_result(f"User endpoint {route}", False, "Not found")
        
        # Check dataset endpoints
        dataset_routes = [route.path for route in datasets.router.routes]
        expected_dataset_routes = ['/datasets', '/datasets/{dataset_id}', '/datasets/{dataset_id}/permissions']
        
        for route in expected_dataset_routes:
            if any(route in r for r in dataset_routes):
                self.log_result(f"Dataset endpoint {route}", True, "Defined")
            else:
                self.log_result(f"Dataset endpoint {route}", False, "Not found")
        
        # Check versioning endpoints including new table endpoints
        versioning_routes = [route.path for route in versioning.router.routes]
        expected_versioning_routes = [
            '/datasets/{dataset_id}/refs/{ref_name}/commits',
            '/datasets/{dataset_id}/refs/{ref_name}/data',
            '/datasets/{dataset_id}/commits/{commit_id}/schema',
            '/datasets/{dataset_id}/refs/{ref_name}/tables',
            '/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/data',
            '/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/schema'
        ]
        
        for route in expected_versioning_routes:
            if any(route in r for r in versioning_routes):
                self.log_result(f"Versioning endpoint {route}", True, "Defined")
            else:
                self.log_result(f"Versioning endpoint {route}", False, "Not found")
    
    async def test_multi_table_support(self):
        """Test multi-table file processing logic."""
        print("\n📊 Testing Multi-Table Support...")
        
        try:
            from src.features.jobs.process_import_job import ProcessImportJobHandler
        except ImportError:
            from features.jobs.process_import_job import ProcessImportJobHandler
        
        # Create handler with mocks
        mock_uow = Mock()
        mock_job_repo = Mock()
        mock_commit_repo = Mock()
        handler = ProcessImportJobHandler(mock_uow, mock_job_repo, mock_commit_repo)
        
        # Test data type mapping
        try:
            assert handler._map_dtype_to_type('int64') == 'integer'
            assert handler._map_dtype_to_type('float64') == 'number'
            assert handler._map_dtype_to_type('datetime64[ns]') == 'datetime'
            assert handler._map_dtype_to_type('object') == 'string'
            self.log_result("Data type mapping", True, "All types mapped correctly")
        except Exception as e:
            self.log_result("Data type mapping", False, str(e))
        
        # Test statistics calculation
        try:
            manifest = [
                ('Revenue:0', 'hash1'),
                ('Revenue:1', 'hash2'),
                ('Expenses:0', 'hash3')
            ]
            schema_def = {
                'Revenue': {'columns': {'amount': {}, 'date': {}}},
                'Expenses': {'columns': {'category': {}, 'amount': {}}}
            }
            stats = handler._calculate_statistics(set(), manifest, schema_def)
            assert stats['Revenue']['row_count'] == 2
            assert stats['Expenses']['row_count'] == 1
            assert stats['Revenue']['columns'] == 2
            self.log_result("Statistics calculation", True, "Correctly calculates per-table stats")
        except Exception as e:
            self.log_result("Statistics calculation", False, str(e))
    
    async def test_table_reader_implementation(self):
        """Test the ITableReader implementation."""
        print("\n📖 Testing ITableReader Implementation...")
        
        try:
            from src.core.services.postgres.table_reader import PostgresTableReader
        except ImportError:
            from core.services.postgres.table_reader import PostgresTableReader
        
        # Mock connection
        mock_conn = AsyncMock()
        reader = PostgresTableReader(mock_conn)
        
        # Test list_table_keys
        try:
            mock_conn.fetch.return_value = [
                {'table_key': 'Revenue'},
                {'table_key': 'Expenses'}
            ]
            tables = await reader.list_table_keys('commit123')
            assert tables == ['Revenue', 'Expenses']
            self.log_result("ITableReader.list_table_keys", True, "Lists table keys correctly")
        except Exception as e:
            self.log_result("ITableReader.list_table_keys", False, str(e))
        
        # Test count_table_rows
        try:
            mock_conn.fetchval.return_value = 100
            count = await reader.count_table_rows('commit123', 'Revenue')
            assert count == 100
            self.log_result("ITableReader.count_table_rows", True, "Counts rows correctly")
        except Exception as e:
            self.log_result("ITableReader.count_table_rows", False, str(e))
    
    def print_summary(self):
        """Print test summary."""
        print("\n" + "="*60)
        print("📊 MOCK TEST SUMMARY")
        print("="*60)
        
        total = len(self.results)
        passed = sum(1 for _, success in self.results if success)
        failed = total - passed
        
        print(f"\nTotal Tests: {total}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        
        if failed > 0:
            print("\n❌ Failed Tests:")
            for name, success in self.results:
                if not success:
                    print(f"  - {name}")
        
        print("\n" + "="*60)
        return failed == 0
    
    async def run_all_tests(self):
        """Run all mock tests."""
        print("🚀 Starting DSA Platform Mock API Tests")
        print("="*60)
        
        try:
            await self.test_endpoint_definitions()
            await self.test_table_handlers()
            await self.test_multi_table_support()
            await self.test_table_reader_implementation()
            
            return self.print_summary()
        except Exception as e:
            print(f"\n❌ Test execution failed: {e}")
            import traceback
            traceback.print_exc()
            return False


async def main():
    """Main test runner."""
    tester = MockAPITester()
    success = await tester.run_all_tests()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())