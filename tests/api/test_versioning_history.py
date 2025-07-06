"""Integration tests for versioning history and checkout endpoints."""

import pytest
from httpx import AsyncClient
from unittest.mock import patch, AsyncMock, Mock
from datetime import datetime

from src.models.pydantic_models import CurrentUser


@pytest.fixture
def mock_current_user():
    """Create a mock current user."""
    return CurrentUser(
        soeid="TEST001",
        user_id=1,
        role_id=1,
        role_name="admin"
    )


@pytest.mark.asyncio
async def test_get_commit_history_success(client: AsyncClient, mock_current_user):
    """Test successful retrieval of commit history."""
    # Mock dependencies
    with patch('src.api.versioning.get_current_user_info', return_value=mock_current_user), \
         patch('src.api.versioning.UnitOfWorkFactory') as mock_uow_factory:
        
        # Setup mock UOW
        mock_uow = Mock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock()
        mock_uow_factory.return_value.create.return_value = mock_uow
        
        # Mock permission check
        mock_uow.datasets.check_user_permission = AsyncMock(return_value=True)
        
        # Mock commit history
        mock_commits = [
            {
                'commit_id': 'abc123',
                'parent_commit_id': 'parent123',
                'message': 'Updated data',
                'author_id': 1,
                'created_at': datetime.now(),
                'row_count': 150
            },
            {
                'commit_id': 'parent123',
                'parent_commit_id': None,
                'message': 'Initial import',
                'author_id': 1,
                'created_at': datetime.now(),
                'row_count': 100
            }
        ]
        
        mock_uow.commits.get_commit_history = AsyncMock(return_value=mock_commits)
        mock_uow.commits.count_commits_for_dataset = AsyncMock(return_value=2)
        mock_uow.users.get_by_id = AsyncMock(return_value={'soeid': 'TEST001'})
        
        # Make request
        response = await client.get(
            "/api/datasets/1/history",
            headers={"Authorization": "Bearer test_token"}
        )
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data['total'] == 2
        assert len(data['commits']) == 2
        assert data['commits'][0]['commit_id'] == 'abc123'
        assert data['commits'][0]['author_name'] == 'TEST001'
        assert data['commits'][0]['row_count'] == 150
        assert data['commits'][1]['commit_id'] == 'parent123'
        assert data['commits'][1]['parent_commit_id'] is None


@pytest.mark.asyncio
async def test_get_commit_history_with_pagination(client: AsyncClient, mock_current_user):
    """Test commit history with pagination parameters."""
    with patch('src.api.versioning.get_current_user_info', return_value=mock_current_user), \
         patch('src.api.versioning.UnitOfWorkFactory') as mock_uow_factory:
        
        # Setup mock UOW
        mock_uow = Mock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock()
        mock_uow_factory.return_value.create.return_value = mock_uow
        
        mock_uow.datasets.check_user_permission = AsyncMock(return_value=True)
        mock_uow.commits.get_commit_history = AsyncMock(return_value=[])
        mock_uow.commits.count_commits_for_dataset = AsyncMock(return_value=100)
        
        # Make request with pagination
        response = await client.get(
            "/api/datasets/1/history?offset=20&limit=10",
            headers={"Authorization": "Bearer test_token"}
        )
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data['total'] == 100
        assert data['offset'] == 20
        assert data['limit'] == 10


@pytest.mark.asyncio
async def test_get_commit_history_no_permission(client: AsyncClient, mock_current_user):
    """Test commit history without permission."""
    # Set user as non-admin
    mock_user = CurrentUser(
        soeid="USER001",
        user_id=2,
        role_id=3,
        role_name="user"
    )
    
    with patch('src.api.versioning.get_current_user_info', return_value=mock_user), \
         patch('src.api.versioning.UnitOfWorkFactory') as mock_uow_factory:
        
        mock_uow = Mock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock()
        mock_uow_factory.return_value.create.return_value = mock_uow
        
        # User has no permission
        mock_uow.datasets.check_user_permission = AsyncMock(return_value=False)
        
        # Make request
        response = await client.get(
            "/api/datasets/1/history",
            headers={"Authorization": "Bearer test_token"}
        )
        
        # Assert
        assert response.status_code == 403
        assert response.json()['detail'] == "You don't have permission to view this dataset"


@pytest.mark.asyncio
async def test_checkout_commit_success(client: AsyncClient, mock_current_user):
    """Test successful checkout of a commit."""
    with patch('src.api.versioning.get_current_user_info', return_value=mock_current_user), \
         patch('src.api.versioning.UnitOfWorkFactory') as mock_uow_factory:
        
        # Setup mock UOW
        mock_uow = Mock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock()
        mock_uow_factory.return_value.create.return_value = mock_uow
        
        mock_uow.datasets.check_user_permission = AsyncMock(return_value=True)
        
        # Mock commit data
        mock_commit = {
            'commit_id': 'abc123',
            'dataset_id': 1
        }
        
        mock_data_rows = [
            {
                'logical_row_id': 'Sheet1:0',
                'data': {'id': 1, 'name': 'Test', 'value': 100}
            }
        ]
        
        mock_uow.commits.get_commit_by_id = AsyncMock(return_value=mock_commit)
        mock_uow.commits.get_commit_data = AsyncMock(return_value=mock_data_rows)
        mock_uow.commits.count_commit_rows = AsyncMock(return_value=1)
        
        # Make request
        response = await client.get(
            "/api/datasets/1/commits/abc123/data",
            headers={"Authorization": "Bearer test_token"}
        )
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data['commit_id'] == 'abc123'
        assert data['total_rows'] == 1
        assert len(data['data']) == 1
        assert data['data'][0]['id'] == 1
        assert data['data'][0]['_logical_row_id'] == 'Sheet1:0'


@pytest.mark.asyncio
async def test_checkout_commit_with_table_filter(client: AsyncClient, mock_current_user):
    """Test checkout with table key filter."""
    with patch('src.api.versioning.get_current_user_info', return_value=mock_current_user), \
         patch('src.api.versioning.UnitOfWorkFactory') as mock_uow_factory:
        
        mock_uow = Mock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock()
        mock_uow_factory.return_value.create.return_value = mock_uow
        
        mock_uow.datasets.check_user_permission = AsyncMock(return_value=True)
        mock_uow.commits.get_commit_by_id = AsyncMock(return_value={'commit_id': 'abc123', 'dataset_id': 1})
        mock_uow.commits.get_commit_data = AsyncMock(return_value=[])
        mock_uow.commits.count_commit_rows = AsyncMock(return_value=0)
        
        # Make request with table filter
        response = await client.get(
            "/api/datasets/1/commits/abc123/data?table_key=Revenue",
            headers={"Authorization": "Bearer test_token"}
        )
        
        # Assert
        assert response.status_code == 200
        # Verify the handler was called with table_key
        mock_uow.commits.get_commit_data.assert_called_once()
        call_args = mock_uow.commits.get_commit_data.call_args
        assert call_args[1]['sheet_name'] == 'Revenue'


@pytest.mark.asyncio
async def test_checkout_commit_not_found(client: AsyncClient, mock_current_user):
    """Test checkout of non-existent commit."""
    with patch('src.api.versioning.get_current_user_info', return_value=mock_current_user), \
         patch('src.api.versioning.UnitOfWorkFactory') as mock_uow_factory:
        
        mock_uow = Mock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock()
        mock_uow_factory.return_value.create.return_value = mock_uow
        
        mock_uow.datasets.check_user_permission = AsyncMock(return_value=True)
        mock_uow.commits.get_commit_by_id = AsyncMock(return_value=None)
        
        # Make request
        response = await client.get(
            "/api/datasets/1/commits/nonexistent/data",
            headers={"Authorization": "Bearer test_token"}
        )
        
        # Assert
        assert response.status_code == 404
        assert "Commit not found" in response.json()['detail']


@pytest.mark.asyncio
async def test_checkout_commit_pagination(client: AsyncClient, mock_current_user):
    """Test checkout with pagination parameters."""
    with patch('src.api.versioning.get_current_user_info', return_value=mock_current_user), \
         patch('src.api.versioning.UnitOfWorkFactory') as mock_uow_factory:
        
        mock_uow = Mock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock()
        mock_uow_factory.return_value.create.return_value = mock_uow
        
        mock_uow.datasets.check_user_permission = AsyncMock(return_value=True)
        mock_uow.commits.get_commit_by_id = AsyncMock(return_value={'commit_id': 'abc123', 'dataset_id': 1})
        mock_uow.commits.get_commit_data = AsyncMock(return_value=[])
        mock_uow.commits.count_commit_rows = AsyncMock(return_value=1000)
        
        # Make request with pagination
        response = await client.get(
            "/api/datasets/1/commits/abc123/data?offset=100&limit=50",
            headers={"Authorization": "Bearer test_token"}
        )
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data['offset'] == 100
        assert data['limit'] == 50
        assert data['total_rows'] == 1000