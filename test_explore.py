import asyncio
import json
import pandas as pd
import sys
from io import BytesIO
from enum import Enum

# Add project root to path
sys.path.append("/home/saketh/Projects/dsa")

# Define ProfileFormat enum
class ProfileFormat(str, Enum):
    """Format options for the profile output"""
    JSON = "json"
    HTML = "html"

# Mock repository for testing
class MockRepository:
    async def get_dataset_version(self, version_id):
        return {
            "version_id": version_id,
            "dataset_id": 1,
            "file_id": 1
        }

    async def get_file(self, file_id):
        # Create a sample DataFrame
        df = pd.DataFrame({
            'integer_col': [1, 2, 3, 4, 5, None, 7, 8, 9, 10],
            'float_col': [1.1, 2.2, None, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.10],
            'string_col': ['a', 'b', 'c', None, 'e', 'f', 'g', 'h', 'i', 'j'],
            'category_col': ['cat1', 'cat2', 'cat1', 'cat2', 'cat1', None, 'cat2', 'cat1', 'cat2', 'cat1']
        })

        # Save to CSV in memory
        buffer = BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        return {
            "file_id": file_id,
            "file_type": "csv",
            "file_data": buffer.getvalue()
        }

# Import the explore service code directly to avoid import issues
from src.app.explore.service import ExploreService

# Create a test request object
class TestRequest:
    def __init__(self, operations, format=ProfileFormat.JSON, sheet=None):
        self.operations = operations
        self.format = format
        self.sheet = sheet

async def test_explore_basic():
    """Basic test with no operations"""
    repo = MockRepository()
    service = ExploreService(repo)
    
    request = TestRequest(operations=[])
    result = await service.explore_dataset(dataset_id=1, version_id=1, request=request, user_id=1)
    
    assert result["format"] == "json"
    assert "profile" in result
    print("Basic test passed")

async def test_explore_filter():
    """Test filtering rows"""
    repo = MockRepository()
    service = ExploreService(repo)
    
    request = TestRequest(operations=[
        {"type": "filter_rows", "expression": "integer_col > 5"}
    ])
    result = await service.explore_dataset(dataset_id=1, version_id=1, request=request, user_id=1)
    
    assert result["format"] == "json"
    assert "profile" in result
    print("Filter test passed")

async def test_explore_remove_nulls():
    """Test removing null values"""
    repo = MockRepository()
    service = ExploreService(repo)
    
    request = TestRequest(operations=[
        {"type": "remove_nulls"}
    ])
    result = await service.explore_dataset(dataset_id=1, version_id=1, request=request, user_id=1)
    
    assert result["format"] == "json"
    assert "profile" in result
    print("Remove nulls test passed")

async def test_explore_rename_columns():
    """Test renaming columns"""
    repo = MockRepository()
    service = ExploreService(repo)
    
    request = TestRequest(operations=[
        {"type": "rename_columns", "mappings": {"integer_col": "renamed_int", "float_col": "renamed_float"}}
    ])
    result = await service.explore_dataset(dataset_id=1, version_id=1, request=request, user_id=1)
    
    assert result["format"] == "json"
    assert "profile" in result
    print("Rename columns test passed")

async def test_explore_sort():
    """Test sorting data"""
    repo = MockRepository()
    service = ExploreService(repo)
    
    request = TestRequest(operations=[
        {"type": "sort_rows", "columns": ["integer_col"], "order": ["desc"]}
    ])
    result = await service.explore_dataset(dataset_id=1, version_id=1, request=request, user_id=1)
    
    assert result["format"] == "json"
    assert "profile" in result
    print("Sort test passed")

async def test_explore_html_format():
    """Test HTML output format"""
    repo = MockRepository()
    service = ExploreService(repo)
    
    request = TestRequest(operations=[], format=ProfileFormat.HTML)
    result = await service.explore_dataset(dataset_id=1, version_id=1, request=request, user_id=1)
    
    assert result["format"] == "html"
    assert "profile" in result
    assert isinstance(result["profile"], str)
    assert "<!DOCTYPE html>" in result["profile"] or "<html" in result["profile"]
    print("HTML format test passed")

async def test_complex_operations():
    """Test multiple operations combined"""
    repo = MockRepository()
    service = ExploreService(repo)
    
    request = TestRequest(operations=[
        {"type": "filter_rows", "expression": "integer_col > 2"},
        {"type": "remove_nulls", "columns": ["string_col"]},
        {"type": "rename_columns", "mappings": {"integer_col": "id"}},
        {"type": "derive_column", "column": "double_id", "expression": "df['id'] * 2"},
        {"type": "sort_rows", "columns": ["id"], "order": ["asc"]}
    ])
    result = await service.explore_dataset(dataset_id=1, version_id=1, request=request, user_id=1)
    
    assert result["format"] == "json"
    assert "profile" in result
    print("Complex operations test passed")

async def run_tests():
    """Run all tests"""
    print("Running explore functionality tests...")
    await test_explore_basic()
    await test_explore_filter()
    await test_explore_remove_nulls()
    await test_explore_rename_columns()
    await test_explore_sort()
    await test_explore_html_format()
    await test_complex_operations()
    print("All tests passed!")

if __name__ == "__main__":
    # Run the tests
    asyncio.run(run_tests())