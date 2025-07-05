"""Unit tests for statistics calculator."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date

from src.core.infrastructure.services.statistics.calculator import DefaultStatisticsCalculator
from src.core.abstractions.services import (
    TableStatistics, ColumnStatistics
)


class TestDefaultStatisticsCalculator:
    """Test default statistics calculator implementation."""
    
    @pytest.fixture
    def calculator(self):
        return DefaultStatisticsCalculator()
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample dataframe with various data types."""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', None, 'Eve'],
            'age': [25, 30, 35, 40, None],
            'salary': [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
            'is_active': [True, False, True, True, False],
            'join_date': pd.to_datetime(['2020-01-01', '2020-02-01', '2020-03-01', '2020-04-01', '2020-05-01']),
            'category': pd.Categorical(['A', 'B', 'A', 'B', 'C'])
        })
    
    @pytest.fixture
    def numeric_series(self):
        """Create a numeric series for testing."""
        return pd.Series([1, 2, 3, 4, 5, None, 7, 8, 9, 10])
    
    @pytest.fixture
    def string_series(self):
        """Create a string series for testing."""
        return pd.Series(['apple', 'banana', 'cherry', None, 'date', 'elderberry'])
    
    @pytest.fixture
    def datetime_series(self):
        """Create a datetime series for testing."""
        return pd.to_datetime([
            '2020-01-01', '2020-02-01', '2020-03-01', 
            None, '2020-05-01', '2020-06-01'
        ])
    
    @pytest.mark.asyncio
    async def test_calculate_numeric_column_statistics(self, calculator, numeric_series):
        """Test calculating statistics for numeric column."""
        stats = await calculator.calculate_column_statistics(numeric_series, 'test_numeric')
        
        assert isinstance(stats, ColumnStatistics)
        assert stats.name == 'test_numeric'
        assert stats.dtype == 'number'
        assert stats.null_count == 1
        assert stats.null_percentage == 10.0
        assert stats.unique_count == 9
        assert stats.min_value == 1.0
        assert stats.max_value == 10.0
        assert stats.mean_value == pytest.approx(5.5, rel=1e-2)
        assert stats.median_value == 5.0
        assert stats.std_dev is not None
    
    @pytest.mark.asyncio
    async def test_calculate_string_column_statistics(self, calculator, string_series):
        """Test calculating statistics for string column."""
        stats = await calculator.calculate_column_statistics(string_series, 'test_string')
        
        assert stats.name == 'test_string'
        assert stats.dtype == 'string'
        assert stats.null_count == 1
        assert stats.null_percentage == pytest.approx(16.67, rel=1e-2)
        assert stats.unique_count == 5
        assert stats.min_value == 'apple'  # Alphabetically first
        assert stats.max_value == 'elderberry'  # Alphabetically last
        assert stats.mean_value is None  # Not applicable for strings
        assert stats.median_value is None
        assert stats.std_dev is None
    
    @pytest.mark.asyncio
    async def test_calculate_datetime_column_statistics(self, calculator, datetime_series):
        """Test calculating statistics for datetime column."""
        stats = await calculator.calculate_column_statistics(datetime_series, 'test_datetime')
        
        assert stats.name == 'test_datetime'
        assert stats.dtype == 'datetime'
        assert stats.null_count == 1
        assert stats.min_value == '2020-01-01T00:00:00'
        assert stats.max_value == '2020-06-01T00:00:00'
        assert stats.mean_value is None
        assert stats.median_value is None
    
    @pytest.mark.asyncio
    async def test_calculate_boolean_column_statistics(self, calculator):
        """Test calculating statistics for boolean column."""
        bool_series = pd.Series([True, False, True, None, False, True])
        stats = await calculator.calculate_column_statistics(bool_series, 'test_bool')
        
        assert stats.name == 'test_bool'
        assert stats.dtype == 'boolean'
        assert stats.null_count == 1
        assert stats.unique_count == 2  # True and False
    
    @pytest.mark.asyncio
    async def test_calculate_table_statistics(self, calculator, sample_dataframe):
        """Test calculating statistics for entire table."""
        stats = await calculator.calculate_table_statistics(sample_dataframe, 'test_table')
        
        assert isinstance(stats, TableStatistics)
        assert stats.row_count == 5
        assert stats.column_count == 7
        assert stats.memory_usage_bytes > 0
        assert stats.unique_row_count == 5  # All rows are unique
        assert stats.duplicate_row_count == 0
        
        # Check that all columns have statistics
        assert len(stats.columns) == 7
        assert 'id' in stats.columns
        assert 'name' in stats.columns
        assert 'age' in stats.columns
        assert 'salary' in stats.columns
        assert 'is_active' in stats.columns
        assert 'join_date' in stats.columns
        assert 'category' in stats.columns
        
        # Check specific column stats
        assert stats.columns['id'].dtype == 'integer'
        assert stats.columns['name'].dtype == 'string'
        assert stats.columns['age'].dtype == 'number'  # Has NaN
        assert stats.columns['salary'].dtype == 'number'
        assert stats.columns['is_active'].dtype == 'boolean'
        assert stats.columns['join_date'].dtype == 'datetime'
        assert stats.columns['category'].dtype == 'category'
    
    @pytest.mark.asyncio
    async def test_table_with_duplicates(self, calculator):
        """Test statistics for table with duplicate rows."""
        df = pd.DataFrame({
            'col1': [1, 2, 1, 2],
            'col2': ['a', 'b', 'a', 'b']
        })
        
        stats = await calculator.calculate_table_statistics(df, 'test_duplicates')
        
        assert stats.row_count == 4
        assert stats.unique_row_count == 2
        assert stats.duplicate_row_count == 2
    
    def test_get_summary_dict(self, calculator):
        """Test converting statistics to dictionary."""
        # Create mock statistics
        col_stats = ColumnStatistics(
            name='test_col',
            dtype='integer',
            null_count=5,
            null_percentage=10.0,
            unique_count=50,
            min_value=1,
            max_value=100,
            mean_value=50.5,
            median_value=50,
            std_dev=28.87
        )
        
        table_stats = TableStatistics(
            row_count=100,
            column_count=1,
            columns={'test_col': col_stats},
            memory_usage_bytes=1024,
            unique_row_count=95,
            duplicate_row_count=5
        )
        
        summary = calculator.get_summary_dict(table_stats)
        
        assert isinstance(summary, dict)
        assert summary['row_count'] == 100
        assert summary['column_count'] == 1
        assert summary['memory_usage_bytes'] == 1024
        assert summary['unique_row_count'] == 95
        assert summary['duplicate_row_count'] == 5
        
        # Check column stats
        assert 'columns' in summary
        assert 'test_col' in summary['columns']
        col_summary = summary['columns']['test_col']
        assert col_summary['dtype'] == 'integer'
        assert col_summary['null_count'] == 5
        assert col_summary['null_percentage'] == 10.0
        assert col_summary['unique_count'] == 50
        assert col_summary['min_value'] == 1
        assert col_summary['max_value'] == 100
        assert col_summary['mean_value'] == 50.5
        assert col_summary['median_value'] == 50
        assert col_summary['std_dev'] == 28.87
    
    def test_map_dtype_to_type(self, calculator):
        """Test dtype mapping."""
        assert calculator._map_dtype_to_type('int64') == 'integer'
        assert calculator._map_dtype_to_type('int32') == 'integer'
        assert calculator._map_dtype_to_type('float64') == 'number'
        assert calculator._map_dtype_to_type('float32') == 'number'
        assert calculator._map_dtype_to_type('bool') == 'boolean'
        assert calculator._map_dtype_to_type('datetime64[ns]') == 'datetime'
        assert calculator._map_dtype_to_type('timedelta64[ns]') == 'timedelta'
        assert calculator._map_dtype_to_type('category') == 'category'
        assert calculator._map_dtype_to_type('object') == 'string'
        assert calculator._map_dtype_to_type('string') == 'string'