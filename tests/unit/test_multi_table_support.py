"""Unit tests for multi-table file ingestion support."""

import pytest
import json
from unittest.mock import Mock, patch, AsyncMock
from src.features.jobs.process_import_job import ProcessImportJobHandler


class TestMultiTableSupport:
    """Test cases for multi-table file handling."""
    
    @pytest.fixture
    def handler(self):
        """Create a handler with mocked dependencies."""
        uow = Mock()
        job_repo = Mock()
        commit_repo = Mock()
        parser_factory = Mock()
        stats_calculator = Mock()
        return ProcessImportJobHandler(uow, job_repo, commit_repo, parser_factory, stats_calculator)
    
    def test_map_dtype_to_type(self, handler):
        """Test data type mapping."""
        assert handler._map_dtype_to_type('int64') == 'integer'
        assert handler._map_dtype_to_type('float64') == 'number'
        assert handler._map_dtype_to_type('bool') == 'boolean'
        assert handler._map_dtype_to_type('datetime64[ns]') == 'datetime'
        assert handler._map_dtype_to_type('object') == 'string'
        assert handler._map_dtype_to_type('unknown') == 'string'
    
    def test_calculate_statistics_single_table(self, handler):
        """Test statistics calculation for single table (Parquet/CSV)."""
        rows_to_store = {('hash1', 'data1'), ('hash2', 'data2')}
        manifest = [
            ('primary:0', 'hash1'),
            ('primary:1', 'hash2')
        ]
        schema_def = {
            'primary': {
                'columns': {
                    'col1': {'type': 'string'},
                    'col2': {'type': 'integer'}
                }
            }
        }
        
        stats = handler._calculate_statistics(rows_to_store, manifest, schema_def)
        
        assert 'primary' in stats
        assert stats['primary']['row_count'] == 2
        assert stats['primary']['columns'] == 2
        assert stats['primary']['unique_rows'] == 2
    
    def test_calculate_statistics_multi_table(self, handler):
        """Test statistics calculation for multi-table (Excel)."""
        rows_to_store = {('hash1', 'data1'), ('hash2', 'data2'), ('hash3', 'data3')}
        manifest = [
            ('Revenue:0', 'hash1'),
            ('Revenue:1', 'hash2'),
            ('Expenses:0', 'hash3')
        ]
        schema_def = {
            'Revenue': {
                'columns': {
                    'date': {'type': 'datetime'},
                    'amount': {'type': 'number'}
                }
            },
            'Expenses': {
                'columns': {
                    'category': {'type': 'string'},
                    'amount': {'type': 'number'},
                    'date': {'type': 'datetime'}
                }
            }
        }
        
        stats = handler._calculate_statistics(rows_to_store, manifest, schema_def)
        
        assert 'Revenue' in stats
        assert 'Expenses' in stats
        assert stats['Revenue']['row_count'] == 2
        assert stats['Revenue']['columns'] == 2
        assert stats['Expenses']['row_count'] == 1
        assert stats['Expenses']['columns'] == 3
        assert stats['Revenue']['unique_rows'] == 3  # Total unique rows across all tables
    
    @pytest.mark.asyncio
    @patch('pandas.read_csv')
    @patch('pandas.read_parquet')
    @patch('pandas.read_excel')
    @patch('os.path.splitext')
    async def test_parse_file_csv(self, mock_splitext, mock_read_excel, 
                                  mock_read_parquet, mock_read_csv, handler):
        """Test parsing CSV file with 'primary' table key."""
        # Mock file extension detection
        mock_splitext.return_value = ('test', '.csv')
        
        # Mock CSV data
        mock_df = Mock()
        mock_df.columns = ['col1', 'col2']
        mock_df.__len__ = Mock(return_value=2)
        mock_df.iterrows.return_value = [
            (0, Mock(to_dict=lambda: {'col1': 'a', 'col2': 1})),
            (1, Mock(to_dict=lambda: {'col1': 'b', 'col2': 2}))
        ]
        mock_df.__getitem__ = lambda self, col: Mock(
            dtype='object' if col == 'col1' else 'int64',
            isnull=lambda: Mock(any=lambda: False)
        )
        mock_read_csv.return_value = mock_df
        
        rows, manifest, schema = await handler._parse_file('/tmp/test.csv', 'test.csv')
        
        # Verify CSV uses 'primary' as table key
        assert 'primary' in schema
        assert len(manifest) == 2
        assert manifest[0][0] == 'primary:0'
        assert manifest[1][0] == 'primary:1'
        assert schema['primary']['row_count'] == 2
        assert schema['primary']['columns']['col1']['type'] == 'string'
        assert schema['primary']['columns']['col2']['type'] == 'integer'
    
    @pytest.mark.asyncio
    @patch('pandas.read_csv')
    @patch('pandas.read_parquet')
    @patch('pandas.read_excel')
    @patch('os.path.splitext')
    async def test_parse_file_parquet(self, mock_splitext, mock_read_excel, 
                                      mock_read_parquet, mock_read_csv, handler):
        """Test parsing Parquet file with 'primary' table key."""
        # Mock file extension detection
        mock_splitext.return_value = ('test', '.parquet')
        
        # Mock Parquet data (same structure as CSV)
        mock_df = Mock()
        mock_df.columns = ['col1', 'col2']
        mock_df.__len__ = Mock(return_value=2)
        mock_df.iterrows.return_value = [
            (0, Mock(to_dict=lambda: {'col1': 'a', 'col2': 1})),
            (1, Mock(to_dict=lambda: {'col1': 'b', 'col2': 2}))
        ]
        mock_df.__getitem__ = lambda self, col: Mock(
            dtype='object' if col == 'col1' else 'int64',
            isnull=lambda: Mock(any=lambda: False)
        )
        mock_read_parquet.return_value = mock_df
        
        rows, manifest, schema = await handler._parse_file('/tmp/test.parquet', 'test.parquet')
        
        # Verify Parquet uses 'primary' as table key
        assert 'primary' in schema
        assert len(manifest) == 2
        assert manifest[0][0] == 'primary:0'
        assert manifest[1][0] == 'primary:1'
    
    @pytest.mark.asyncio
    @patch('pandas.read_csv')
    @patch('pandas.read_parquet')
    @patch('pandas.read_excel')
    @patch('os.path.splitext')
    async def test_parse_file_excel_multi_sheet(self, mock_splitext, mock_read_excel, 
                                                mock_read_parquet, mock_read_csv, handler):
        """Test parsing Excel file with multiple sheets."""
        # Mock file extension detection
        mock_splitext.return_value = ('test', '.xlsx')
        
        # Mock Excel data with multiple sheets
        mock_df1 = Mock()
        mock_df1.columns = ['date', 'revenue']
        mock_df1.__len__ = Mock(return_value=1)
        mock_df1.iterrows.return_value = [
            (0, Mock(to_dict=lambda: {'date': '2024-01-01', 'revenue': 1000}))
        ]
        mock_df1.__getitem__ = lambda self, col: Mock(
            dtype='datetime64[ns]' if col == 'date' else 'float64',
            isnull=lambda: Mock(any=lambda: False)
        )
        
        mock_df2 = Mock()
        mock_df2.columns = ['category', 'amount']
        mock_df2.__len__ = Mock(return_value=1)
        mock_df2.iterrows.return_value = [
            (0, Mock(to_dict=lambda: {'category': 'Office', 'amount': 500}))
        ]
        mock_df2.__getitem__ = lambda self, col: Mock(
            dtype='object' if col == 'category' else 'float64',
            isnull=lambda: Mock(any=lambda: False)
        )
        
        mock_read_excel.return_value = {
            'Revenue': mock_df1,
            'Expenses': mock_df2
        }
        
        rows, manifest, schema = await handler._parse_file('/tmp/test.xlsx', 'test.xlsx')
        
        # Verify Excel uses sheet names as table keys
        assert 'Revenue' in schema
        assert 'Expenses' in schema
        assert len(manifest) == 2
        
        # Check that logical_row_ids use sheet names
        logical_ids = [m[0] for m in manifest]
        assert any('Revenue:' in lid for lid in logical_ids)
        assert any('Expenses:' in lid for lid in logical_ids)
        
        # Verify schemas
        assert schema['Revenue']['columns']['date']['type'] == 'datetime'
        assert schema['Revenue']['columns']['revenue']['type'] == 'number'
        assert schema['Expenses']['columns']['category']['type'] == 'string'
        assert schema['Expenses']['columns']['amount']['type'] == 'number'