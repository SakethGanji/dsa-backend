"""Integration tests for table-specific API endpoints."""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi.testclient import TestClient
from src.features.versioning.get_table_data import (
    GetTableDataHandler, ListTablesHandler, GetTableSchemaHandler
)


class TestTableAPI:
    """Test table-specific API endpoints."""
    
    @pytest.mark.asyncio
    async def test_list_tables_single_table(self):
        """Test listing tables for single-table dataset (Parquet/CSV)."""
        # Mock UoW and handler
        mock_uow = AsyncMock()
        mock_uow.__aenter__.return_value = mock_uow
        
        # Mock permission check
        mock_uow.datasets.user_has_permission.return_value = True
        
        # Mock ref lookup
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'abc123'
        }
        
        # Mock table reader
        mock_table_reader = AsyncMock()
        mock_table_reader.list_table_keys.return_value = ['primary']
        
        handler = ListTablesHandler(mock_uow, mock_table_reader)
        result = await handler.handle(
            dataset_id=1,
            ref_name='main',
            user_id=1
        )
        
        assert result == {'tables': ['primary']}
        mock_table_reader.list_table_keys.assert_called_once_with('abc123')
    
    @pytest.mark.asyncio
    async def test_list_tables_multi_table(self):
        """Test listing tables for multi-table dataset (Excel)."""
        # Mock UoW and handler
        mock_uow = AsyncMock()
        mock_uow.__aenter__.return_value = mock_uow
        
        # Mock permission check
        mock_uow.datasets.user_has_permission.return_value = True
        
        # Mock ref lookup
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'abc123'
        }
        
        # Mock table reader
        mock_table_reader = AsyncMock()
        mock_table_reader.list_table_keys.return_value = ['Expenses', 'Revenue']
        
        handler = ListTablesHandler(mock_uow, mock_table_reader)
        result = await handler.handle(
            dataset_id=1,
            ref_name='main',
            user_id=1
        )
        
        assert result == {'tables': ['Expenses', 'Revenue']}
    
    @pytest.mark.asyncio
    async def test_get_table_data_with_pagination(self):
        """Test retrieving paginated data for a specific table."""
        # Mock UoW and handler
        mock_uow = AsyncMock()
        mock_uow.__aenter__.return_value = mock_uow
        
        # Mock permission check
        mock_uow.datasets.user_has_permission.return_value = True
        
        # Mock ref lookup
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'abc123'
        }
        
        # Mock table reader
        mock_table_reader = AsyncMock()
        mock_table_reader.count_table_rows.return_value = 100
        mock_table_reader.get_table_data.return_value = [
            {
                '_row_index': 0,
                '_logical_row_id': 'Revenue:0',
                'date': '2024-01-01',
                'amount': 1000
            },
            {
                '_row_index': 1,
                '_logical_row_id': 'Revenue:1',
                'date': '2024-01-02',
                'amount': 1500
            }
        ]
        
        handler = GetTableDataHandler(mock_uow, mock_table_reader)
        result = await handler.handle(
            dataset_id=1,
            ref_name='main',
            table_key='Revenue',
            user_id=1,
            offset=0,
            limit=2
        )
        
        assert result['table_key'] == 'Revenue'
        assert result['total_count'] == 100
        assert result['offset'] == 0
        assert result['limit'] == 2
        assert len(result['data']) == 2
        assert result['data'][0]['_row_index'] == 0
        assert result['data'][0]['date'] == '2024-01-01'
        assert result['data'][1]['_row_index'] == 1
        assert result['data'][1]['amount'] == 1500
    
    @pytest.mark.asyncio
    async def test_get_table_schema(self):
        """Test retrieving schema for a specific table."""
        # Mock UoW and handler
        mock_uow = AsyncMock()
        mock_uow.__aenter__.return_value = mock_uow
        
        # Mock permission check
        mock_uow.datasets.user_has_permission.return_value = True
        
        # Mock ref lookup
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'abc123'
        }
        
        # Mock table reader
        mock_table_reader = AsyncMock()
        mock_table_reader.get_table_schema.return_value = {
            'columns': {
                'date': {'type': 'datetime', 'nullable': False},
                'amount': {'type': 'number', 'nullable': False}
            },
            'row_count': 100
        }
        
        handler = GetTableSchemaHandler(mock_uow, mock_table_reader)
        result = await handler.handle(
            dataset_id=1,
            ref_name='main',
            table_key='Revenue',
            user_id=1
        )
        
        assert result['table_key'] == 'Revenue'
        assert 'date' in result['schema']['columns']
        assert result['schema']['columns']['date']['type'] == 'datetime'
        assert result['schema']['row_count'] == 100
    
    @pytest.mark.asyncio
    async def test_get_table_data_no_permission(self):
        """Test error when user lacks permission."""
        # Mock UoW and handler
        mock_uow = AsyncMock()
        mock_uow.__aenter__.return_value = mock_uow
        
        # Mock permission check - denied
        mock_uow.datasets.user_has_permission.return_value = False
        
        # Mock table reader
        mock_table_reader = AsyncMock()
        
        handler = GetTableDataHandler(mock_uow, mock_table_reader)
        
        with pytest.raises(Exception) as exc_info:
            await handler.handle(
                dataset_id=1,
                ref_name='main',
                table_key='Revenue',
                user_id=1
            )
        
        assert "permission" in str(exc_info.value).lower()
    
    @pytest.mark.asyncio
    async def test_get_table_schema_not_found(self):
        """Test error when table doesn't exist."""
        # Mock UoW and handler
        mock_uow = AsyncMock()
        mock_uow.__aenter__.return_value = mock_uow
        
        # Mock permission check
        mock_uow.datasets.user_has_permission.return_value = True
        
        # Mock ref lookup
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'abc123'
        }
        
        # Mock table reader
        mock_table_reader = AsyncMock()
        mock_table_reader.get_table_schema.return_value = None
        
        handler = GetTableSchemaHandler(mock_uow, mock_table_reader)
        
        with pytest.raises(Exception) as exc_info:
            await handler.handle(
                dataset_id=1,
                ref_name='main',
                table_key='NonExistent',
                user_id=1
            )
        
        assert "not found" in str(exc_info.value).lower()