"""Unit tests for file parser implementations."""

import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import patch, MagicMock

from src.core.infrastructure.services.file_processing.parsers import (
    CSVParser, ParquetParser, ExcelParser
)
from src.core.abstractions.services import ParsedData, TableData


class TestCSVParser:
    """Test CSV parser implementation."""
    
    @pytest.fixture
    def csv_parser(self):
        return CSVParser()
    
    @pytest.fixture
    def sample_csv_file(self):
        """Create a temporary CSV file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,age,city\n")
            f.write("Alice,30,New York\n")
            f.write("Bob,25,San Francisco\n")
            f.write("Charlie,35,Chicago\n")
            f.name
        yield f.name
        os.unlink(f.name)
    
    def test_can_parse_csv_files(self, csv_parser):
        """Test that CSV parser recognizes CSV files."""
        assert csv_parser.can_parse("data.csv") is True
        assert csv_parser.can_parse("data.CSV") is True
        assert csv_parser.can_parse("file.xlsx") is False
        assert csv_parser.can_parse("file.parquet") is False
    
    def test_get_supported_extensions(self, csv_parser):
        """Test supported extensions list."""
        extensions = csv_parser.get_supported_extensions()
        assert extensions == ['.csv']
    
    @pytest.mark.asyncio
    async def test_parse_csv_file(self, csv_parser, sample_csv_file):
        """Test parsing a CSV file."""
        result = await csv_parser.parse(sample_csv_file, "test.csv")
        
        assert isinstance(result, ParsedData)
        assert result.file_type == 'csv'
        assert result.filename == 'test.csv'
        assert len(result.tables) == 1
        
        table = result.tables[0]
        assert table.table_key == 'primary'
        assert isinstance(table.dataframe, pd.DataFrame)
        assert len(table.dataframe) == 3
        assert list(table.dataframe.columns) == ['name', 'age', 'city']


class TestParquetParser:
    """Test Parquet parser implementation."""
    
    @pytest.fixture
    def parquet_parser(self):
        return ParquetParser()
    
    @pytest.fixture
    def sample_parquet_file(self):
        """Create a temporary Parquet file for testing."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10.5, 20.3, 30.1],
            'category': ['A', 'B', 'A']
        })
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name)
            f.name
        yield f.name
        os.unlink(f.name)
    
    def test_can_parse_parquet_files(self, parquet_parser):
        """Test that Parquet parser recognizes Parquet files."""
        assert parquet_parser.can_parse("data.parquet") is True
        assert parquet_parser.can_parse("data.PARQUET") is True
        assert parquet_parser.can_parse("file.csv") is False
        assert parquet_parser.can_parse("file.xlsx") is False
    
    @pytest.mark.asyncio
    async def test_parse_parquet_file(self, parquet_parser, sample_parquet_file):
        """Test parsing a Parquet file."""
        result = await parquet_parser.parse(sample_parquet_file, "test.parquet")
        
        assert isinstance(result, ParsedData)
        assert result.file_type == 'parquet'
        assert result.filename == 'test.parquet'
        assert len(result.tables) == 1
        
        table = result.tables[0]
        assert table.table_key == 'primary'
        assert len(table.dataframe) == 3
        assert set(table.dataframe.columns) == {'id', 'value', 'category'}


class TestExcelParser:
    """Test Excel parser implementation."""
    
    @pytest.fixture
    def excel_parser(self):
        return ExcelParser()
    
    @pytest.fixture
    def sample_excel_file_single_sheet(self):
        """Create a temporary Excel file with single sheet."""
        df = pd.DataFrame({
            'product': ['A', 'B', 'C'],
            'price': [100, 200, 150],
            'quantity': [10, 5, 8]
        })
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            df.to_excel(f.name, index=False, sheet_name='Products')
            f.name
        yield f.name
        os.unlink(f.name)
    
    @pytest.fixture
    def sample_excel_file_multi_sheet(self):
        """Create a temporary Excel file with multiple sheets."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            with pd.ExcelWriter(f.name) as writer:
                # Revenue sheet
                revenue_df = pd.DataFrame({
                    'month': ['Jan', 'Feb', 'Mar'],
                    'revenue': [1000, 1500, 1200]
                })
                revenue_df.to_excel(writer, sheet_name='Revenue', index=False)
                
                # Expenses sheet
                expenses_df = pd.DataFrame({
                    'category': ['Rent', 'Utilities', 'Salaries'],
                    'amount': [500, 200, 800]
                })
                expenses_df.to_excel(writer, sheet_name='Expenses', index=False)
            f.name
        yield f.name
        os.unlink(f.name)
    
    def test_can_parse_excel_files(self, excel_parser):
        """Test that Excel parser recognizes Excel files."""
        assert excel_parser.can_parse("data.xlsx") is True
        assert excel_parser.can_parse("data.xls") is True
        assert excel_parser.can_parse("data.XLSX") is True
        assert excel_parser.can_parse("file.csv") is False
        assert excel_parser.can_parse("file.parquet") is False
    
    def test_get_supported_extensions(self, excel_parser):
        """Test supported extensions list."""
        extensions = excel_parser.get_supported_extensions()
        assert set(extensions) == {'.xlsx', '.xls'}
    
    @pytest.mark.asyncio
    async def test_parse_single_sheet_excel(self, excel_parser, sample_excel_file_single_sheet):
        """Test parsing an Excel file with single sheet."""
        result = await excel_parser.parse(sample_excel_file_single_sheet, "test.xlsx")
        
        assert isinstance(result, ParsedData)
        assert result.file_type == 'excel'
        assert len(result.tables) == 1
        
        table = result.tables[0]
        assert table.table_key == 'Products'
        assert len(table.dataframe) == 3
        assert list(table.dataframe.columns) == ['product', 'price', 'quantity']
    
    @pytest.mark.asyncio
    async def test_parse_multi_sheet_excel(self, excel_parser, sample_excel_file_multi_sheet):
        """Test parsing an Excel file with multiple sheets."""
        result = await excel_parser.parse(sample_excel_file_multi_sheet, "test.xlsx")
        
        assert isinstance(result, ParsedData)
        assert result.file_type == 'excel'
        assert len(result.tables) == 2
        
        # Check table keys
        table_keys = [table.table_key for table in result.tables]
        assert set(table_keys) == {'Revenue', 'Expenses'}
        
        # Check Revenue sheet
        revenue_table = next(t for t in result.tables if t.table_key == 'Revenue')
        assert len(revenue_table.dataframe) == 3
        assert list(revenue_table.dataframe.columns) == ['month', 'revenue']
        
        # Check Expenses sheet
        expenses_table = next(t for t in result.tables if t.table_key == 'Expenses')
        assert len(expenses_table.dataframe) == 3
        assert list(expenses_table.dataframe.columns) == ['category', 'amount']