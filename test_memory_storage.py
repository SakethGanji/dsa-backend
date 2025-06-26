#!/usr/bin/env python3
"""Test script for in-memory storage backend"""

import os
import sys
import pandas as pd
import polars as pl
from io import BytesIO

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Set environment variable before importing app modules
os.environ['STORAGE_BACKEND'] = 'memory'

from app.storage.memory_backend import InMemoryStorageBackend
from app.storage.factory import StorageFactory


async def test_memory_backend():
    """Test the in-memory storage backend"""
    print("Testing In-Memory Storage Backend with Polars")
    print("=" * 50)
    
    # Test 1: Direct backend instantiation
    print("\n1. Testing direct backend instantiation...")
    backend = InMemoryStorageBackend()
    print("✓ Backend created successfully")
    
    # Test 2: Create test data
    print("\n2. Creating test dataset...")
    test_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'score': [95.5, 87.2, 91.8, 79.3, 88.6],
        'active': [True, True, False, True, False]
    })
    
    # Convert to CSV bytes
    csv_buffer = BytesIO()
    test_data.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue()
    print(f"✓ Created test data with {len(test_data)} rows")
    
    # Test 3: Save dataset
    print("\n3. Saving dataset to memory...")
    result = await backend.save_dataset_file(
        file_content=csv_bytes,
        dataset_id=1,
        version_id=1,
        file_name="test_data.csv"
    )
    print(f"✓ Dataset saved: {result['path']}")
    print(f"  Size: {result['size']} bytes")
    print(f"  Format: {result['format']}")
    
    # Test 4: Read dataset metadata
    print("\n4. Reading dataset metadata...")
    metadata = backend.get_file_metadata(result['path'])
    print(f"✓ Metadata retrieved:")
    print(f"  Rows: {metadata['row_count']}")
    print(f"  Columns: {metadata['column_count']}")
    print(f"  Column names: {', '.join(metadata['columns'][0]['name'] for col in metadata['columns'])}")
    
    # Test 5: Read paginated data
    print("\n5. Reading paginated data...")
    headers, rows, has_more = await backend.read_dataset_paginated(
        file_path=result['path'],
        limit=3,
        offset=0
    )
    print(f"✓ Read {len(rows)} rows")
    print(f"  Headers: {', '.join(headers)}")
    print(f"  First row: {rows[0]}")
    print(f"  Has more: {has_more}")
    
    # Test 6: Get DataFrame directly
    print("\n6. Accessing Polars DataFrame directly...")
    df = backend.get_dataframe(result['path'])
    if df is not None:
        print(f"✓ DataFrame accessed:")
        print(f"  Shape: {df.shape}")
        print(f"  Dtypes: {dict(zip(df.columns, df.dtypes))}")
        print(f"  Memory usage: {df.estimated_size()} bytes")
    
    # Test 7: Factory pattern
    print("\n7. Testing factory pattern...")
    factory = StorageFactory()
    memory_backend = factory.create_backend("memory")
    print(f"✓ Backend created via factory: {type(memory_backend).__name__}")
    
    # Test 8: Test file finalization
    print("\n8. Testing file path finalization...")
    temp_path = "memory://datasets/1/temp/test.csv"
    backend._datasets[temp_path] = df
    new_path = await backend.finalize_file_location(temp_path, 42)
    print(f"✓ Path finalized: {temp_path} -> {new_path}")
    
    # Test 9: List files
    print("\n9. Listing files in memory...")
    files = await backend.list_files()
    print(f"✓ Found {len(files)} files:")
    for f in files:
        print(f"  - {f}")
    
    print("\n✅ All tests passed!")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_memory_backend())