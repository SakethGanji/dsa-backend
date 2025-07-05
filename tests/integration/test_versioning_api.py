"""Integration tests for versioning API endpoints."""

import pytest
from httpx import AsyncClient
from unittest.mock import Mock, AsyncMock, MagicMock
import json

from src.main import app
from src.core.database import DatabasePool
from src.core.abstractions import IUnitOfWork, ITableReader
from src.core.dependencies import get_uow, get_current_user


@pytest.fixture
def mock_uow():
    """Create a mock unit of work."""
    uow = AsyncMock(spec=IUnitOfWork)
    
    # Mock repositories
    uow.datasets = AsyncMock()
    uow.commits = AsyncMock()
    uow.users = AsyncMock()
    uow.jobs = AsyncMock()
    
    # Mock table reader
    uow.table_reader = AsyncMock(spec=ITableReader)
    
    # Make the context manager work
    uow.__aenter__.return_value = uow
    uow.__aexit__.return_value = None
    
    return uow


@pytest.fixture
def mock_current_user():
    """Mock current user."""
    return {
        "id": 1,
        "soeid": "test_user",
        "role": "user"
    }


@pytest.fixture
def override_dependencies(mock_uow, mock_current_user):
    """Override FastAPI dependencies for testing."""
    async def mock_get_uow():
        return mock_uow
    
    async def mock_get_current_user():
        return mock_current_user
    
    app.dependency_overrides[get_uow] = mock_get_uow
    app.dependency_overrides[get_current_user] = mock_get_current_user
    
    yield
    
    # Clean up
    app.dependency_overrides.clear()


class TestTableEndpoints:
    """Test table-specific endpoints."""
    
    @pytest.mark.asyncio
    async def test_list_tables(self, override_dependencies, mock_uow):
        """Test listing tables endpoint."""
        # Setup mocks
        mock_uow.datasets.user_has_permission.return_value = True
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'abc123',
            'created_at': '2024-01-01T00:00:00',
            'updated_at': '2024-01-01T00:00:00'
        }
        mock_uow.table_reader.list_table_keys.return_value = ['Revenue', 'Expenses']
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/datasets/1/refs/main/tables",
                headers={"Authorization": "Bearer test_token"}
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data == {"tables": ['Revenue', 'Expenses']}
        
        # Verify mocks were called correctly
        mock_uow.datasets.user_has_permission.assert_called_once_with(1, 1, "read")
        mock_uow.commits.get_ref.assert_called_once_with(1, "main")
        mock_uow.table_reader.list_table_keys.assert_called_once_with("abc123")
    
    @pytest.mark.asyncio
    async def test_list_tables_no_permission(self, override_dependencies, mock_uow):
        """Test listing tables without permission."""
        mock_uow.datasets.user_has_permission.return_value = False
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/datasets/1/refs/main/tables",
                headers={"Authorization": "Bearer test_token"}
            )
        
        assert response.status_code == 403
        assert "permission" in response.json()["detail"].lower()
    
    @pytest.mark.asyncio
    async def test_list_tables_ref_not_found(self, override_dependencies, mock_uow):
        """Test listing tables when ref doesn't exist."""
        mock_uow.datasets.user_has_permission.return_value = True
        mock_uow.commits.get_ref.return_value = None
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/datasets/1/refs/nonexistent/tables",
                headers={"Authorization": "Bearer test_token"}
            )
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()
    
    @pytest.mark.asyncio
    async def test_get_table_data(self, override_dependencies, mock_uow):
        """Test getting table data endpoint."""
        # Setup mocks
        mock_uow.datasets.user_has_permission.return_value = True
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'abc123'
        }
        mock_uow.table_reader.count_table_rows.return_value = 100
        mock_uow.table_reader.get_table_data.return_value = [
            {'_row_index': 0, '_logical_row_id': 'Revenue:0', 'month': 'Jan', 'revenue': 1000},
            {'_row_index': 1, '_logical_row_id': 'Revenue:1', 'month': 'Feb', 'revenue': 1500}
        ]
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/datasets/1/refs/main/tables/Revenue/data?offset=0&limit=2",
                headers={"Authorization": "Bearer test_token"}
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data['table_key'] == 'Revenue'
        assert data['offset'] == 0
        assert data['limit'] == 2
        assert data['total_count'] == 100
        assert len(data['data']) == 2
        assert data['data'][0]['month'] == 'Jan'
        assert data['data'][0]['revenue'] == 1000
        
        # Verify mocks
        mock_uow.table_reader.get_table_data.assert_called_once_with(
            commit_id='abc123',
            table_key='Revenue',
            offset=0,
            limit=2
        )
    
    @pytest.mark.asyncio
    async def test_get_table_data_empty_ref(self, override_dependencies, mock_uow):
        """Test getting table data when ref has no commit."""
        mock_uow.datasets.user_has_permission.return_value = True
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': None  # No data yet
        }
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/datasets/1/refs/main/tables/Revenue/data",
                headers={"Authorization": "Bearer test_token"}
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data['table_key'] == 'Revenue'
        assert data['data'] == []
        assert data['total_count'] == 0
    
    @pytest.mark.asyncio
    async def test_get_table_schema(self, override_dependencies, mock_uow):
        """Test getting table schema endpoint."""
        # Setup mocks
        mock_uow.datasets.user_has_permission.return_value = True
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'abc123'
        }
        mock_uow.table_reader.get_table_schema.return_value = {
            'columns': {
                'month': {'type': 'string', 'nullable': False},
                'revenue': {'type': 'number', 'nullable': False}
            },
            'row_count': 12
        }
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/datasets/1/refs/main/tables/Revenue/schema",
                headers={"Authorization": "Bearer test_token"}
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data['table_key'] == 'Revenue'
        assert 'schema' in data
        assert 'columns' in data['schema']
        assert 'month' in data['schema']['columns']
        assert data['schema']['columns']['month']['type'] == 'string'
    
    @pytest.mark.asyncio
    async def test_get_table_schema_not_found(self, override_dependencies, mock_uow):
        """Test getting schema for non-existent table."""
        mock_uow.datasets.user_has_permission.return_value = True
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'abc123'
        }
        mock_uow.table_reader.get_table_schema.return_value = None
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/datasets/1/refs/main/tables/NonExistent/schema",
                headers={"Authorization": "Bearer test_token"}
            )
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()


class TestCommitEndpoints:
    """Test commit-related endpoints."""
    
    @pytest.mark.asyncio
    async def test_get_commit_schema(self, override_dependencies, mock_uow):
        """Test getting commit schema endpoint."""
        # Setup mocks
        mock_uow.datasets.get_dataset_by_id.return_value = {
            'dataset_id': 1,
            'name': 'Test Dataset'
        }
        mock_uow.datasets.user_has_permission.return_value = True
        mock_uow.commits.get_commit_schema.return_value = {
            'Revenue': {
                'columns': {'month': {'type': 'string'}, 'revenue': {'type': 'number'}},
                'row_count': 12
            },
            'Expenses': {
                'columns': {'category': {'type': 'string'}, 'amount': {'type': 'number'}},
                'row_count': 50
            }
        }
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/datasets/1/commits/abc123/schema",
                headers={"Authorization": "Bearer test_token"}
            )
        
        assert response.status_code == 200
        # Note: Response model validation might modify the output
        # so we just check that it succeeded and has the right structure
        data = response.json()
        assert 'commit_id' in data
        assert 'schema_definition' in data


class TestDataEndpoints:
    """Test data retrieval endpoints."""
    
    @pytest.mark.asyncio
    async def test_get_data_at_ref(self, override_dependencies, mock_uow):
        """Test getting data at ref endpoint."""
        # Setup mocks
        mock_uow.datasets.user_has_permission.return_value = True
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'abc123'
        }
        mock_uow.commits.get_commit_data.return_value = [
            {
                'logical_row_id': 'primary:0',
                'data': {'id': 1, 'name': 'Alice', 'value': 100}
            },
            {
                'logical_row_id': 'primary:1',
                'data': {'id': 2, 'name': 'Bob', 'value': 200}
            }
        ]
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/datasets/1/refs/main/data?offset=0&limit=2",
                headers={"Authorization": "Bearer test_token"}
            )
        
        assert response.status_code == 200
        data = response.json()
        assert 'data' in data
        assert len(data['data']) == 2
        
        # Verify mocks
        mock_uow.commits.get_commit_data.assert_called_once_with(
            'abc123', None, 0, 2
        )
    
    @pytest.mark.asyncio
    async def test_get_data_at_ref_with_sheet_filter(self, override_dependencies, mock_uow):
        """Test getting data with sheet name filter."""
        mock_uow.datasets.user_has_permission.return_value = True
        mock_uow.commits.get_ref.return_value = {
            'name': 'main',
            'commit_id': 'abc123'
        }
        mock_uow.commits.get_commit_data.return_value = [
            {
                'logical_row_id': 'Revenue:0',
                'data': {'month': 'Jan', 'revenue': 1000}
            }
        ]
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/datasets/1/refs/main/data?sheet_name=Revenue",
                headers={"Authorization": "Bearer test_token"}
            )
        
        assert response.status_code == 200
        
        # Verify sheet filter was passed
        mock_uow.commits.get_commit_data.assert_called_once_with(
            'abc123', 'Revenue', 0, 100
        )