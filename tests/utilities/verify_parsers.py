#!/usr/bin/env python3
"""Simple test script to verify parsers work correctly."""

import sys
import os
import tempfile
import pandas as pd

# Add src to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(project_root, 'src'))

from core.services.file_processing.parsers import CSVParser, ParquetParser, ExcelParser
from core.services.file_processing.factory import FileParserFactory
from core.services.statistics.calculator import DefaultStatisticsCalculator


async def test_csv_parser():
    """Test CSV parser."""
    print("\n🧪 Testing CSV Parser...")
    
    # Create test CSV
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("name,age,city\n")
        f.write("Alice,30,New York\n")
        f.write("Bob,25,San Francisco\n")
        f.write("Charlie,35,Chicago\n")
        csv_file = f.name
    
    try:
        parser = CSVParser()
        assert parser.can_parse("test.csv") == True
        assert parser.can_parse("test.xlsx") == False
        
        result = await parser.parse(csv_file, "test.csv")
        assert len(result.tables) == 1
        assert result.tables[0].table_key == 'primary'
        assert len(result.tables[0].dataframe) == 3
        
        print("✅ CSV Parser tests passed!")
    finally:
        os.unlink(csv_file)


async def test_parquet_parser():
    """Test Parquet parser."""
    print("\n🧪 Testing Parquet Parser...")
    
    # Create test Parquet
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [10.5, 20.3, 30.1]
    })
    
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        df.to_parquet(f.name)
        parquet_file = f.name
    
    try:
        parser = ParquetParser()
        assert parser.can_parse("test.parquet") == True
        assert parser.can_parse("test.csv") == False
        
        result = await parser.parse(parquet_file, "test.parquet")
        assert len(result.tables) == 1
        assert result.tables[0].table_key == 'primary'
        assert len(result.tables[0].dataframe) == 3
        
        print("✅ Parquet Parser tests passed!")
    finally:
        os.unlink(parquet_file)


async def test_excel_parser():
    """Test Excel parser."""
    print("\n🧪 Testing Excel Parser...")
    
    # Create test Excel with multiple sheets
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
        with pd.ExcelWriter(f.name) as writer:
            df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
            df2 = pd.DataFrame({'X': [5, 6], 'Y': [7, 8]})
            df1.to_excel(writer, sheet_name='Sheet1', index=False)
            df2.to_excel(writer, sheet_name='Sheet2', index=False)
        excel_file = f.name
    
    try:
        parser = ExcelParser()
        assert parser.can_parse("test.xlsx") == True
        assert parser.can_parse("test.xls") == True
        assert parser.can_parse("test.csv") == False
        
        result = await parser.parse(excel_file, "test.xlsx")
        assert len(result.tables) == 2
        table_keys = [t.table_key for t in result.tables]
        assert 'Sheet1' in table_keys
        assert 'Sheet2' in table_keys
        
        print("✅ Excel Parser tests passed!")
    finally:
        os.unlink(excel_file)


async def test_parser_factory():
    """Test parser factory."""
    print("\n🧪 Testing Parser Factory...")
    
    factory = FileParserFactory()
    
    # Test getting parsers
    csv_parser = factory.get_parser("data.csv")
    assert isinstance(csv_parser, CSVParser)
    
    parquet_parser = factory.get_parser("data.parquet")
    assert isinstance(parquet_parser, ParquetParser)
    
    excel_parser = factory.get_parser("data.xlsx")
    assert isinstance(excel_parser, ExcelParser)
    
    # Test unsupported format
    try:
        factory.get_parser("data.txt")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unsupported file type" in str(e)
    
    print("✅ Parser Factory tests passed!")


async def test_statistics_calculator():
    """Test statistics calculator."""
    print("\n🧪 Testing Statistics Calculator...")
    
    calc = DefaultStatisticsCalculator()
    
    # Create test dataframe
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['A', 'B', 'C', None, 'D'],
        'value': [10.5, 20.3, 30.1, 40.2, None]
    })
    
    # Test table statistics
    stats = await calc.calculate_table_statistics(df, 'test_table')
    assert stats.row_count == 5
    assert stats.column_count == 3
    assert stats.unique_row_count == 5
    assert stats.duplicate_row_count == 0
    
    # Test column statistics
    assert 'id' in stats.columns
    assert stats.columns['id'].null_count == 0
    assert stats.columns['name'].null_count == 1
    assert stats.columns['value'].null_count == 1
    
    # Test summary dict conversion
    summary = calc.get_summary_dict(stats)
    assert summary['row_count'] == 5
    assert summary['column_count'] == 3
    
    print("✅ Statistics Calculator tests passed!")


async def main():
    """Run all tests."""
    print("=" * 60)
    print("🧪 DSA File Processing Tests")
    print("=" * 60)
    
    try:
        await test_csv_parser()
        await test_parquet_parser()
        await test_excel_parser()
        await test_parser_factory()
        await test_statistics_calculator()
        
        print("\n" + "=" * 60)
        print("🎉 All tests passed!")
        print("=" * 60)
        return 0
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import asyncio
    sys.exit(asyncio.run(main()))