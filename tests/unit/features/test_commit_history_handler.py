"""Unit tests for GetCommitHistoryHandler."""

import pytest
from unittest.mock import Mock, AsyncMock
from datetime import datetime

from src.features.versioning.get_commit_history import GetCommitHistoryHandler
from src.models.pydantic_models import GetCommitHistoryResponse


@pytest.fixture
def mock_uow():
    """Create a mock unit of work."""
    uow = Mock()
    uow.__aenter__ = AsyncMock(return_value=uow)
    uow.__aexit__ = AsyncMock()
    
    # Mock repositories
    uow.commits = Mock()
    uow.users = Mock()
    
    return uow


@pytest.mark.asyncio
async def test_get_commit_history_success(mock_uow):
    """Test successful retrieval of commit history."""
    # Arrange
    dataset_id = 1
    test_commits = [
        {
            'commit_id': 'abc123',
            'parent_commit_id': 'parent123',
            'message': 'Initial commit',
            'author_id': 1,
            'created_at': datetime.now(),
            'row_count': 100
        },
        {
            'commit_id': 'parent123',
            'parent_commit_id': None,
            'message': 'First version',
            'author_id': 2,
            'created_at': datetime.now(),
            'row_count': 50
        }
    ]
    
    test_users = [
        {'soeid': 'user001'},
        {'soeid': 'user002'}
    ]
    
    mock_uow.commits.get_commit_history = AsyncMock(return_value=test_commits)
    mock_uow.commits.count_commits_for_dataset = AsyncMock(return_value=2)
    mock_uow.users.get_by_id = AsyncMock(side_effect=lambda uid: test_users[uid - 1])
    
    handler = GetCommitHistoryHandler(mock_uow)
    
    # Act
    result = await handler.handle(dataset_id, offset=0, limit=50)
    
    # Assert
    assert isinstance(result, GetCommitHistoryResponse)
    assert len(result.commits) == 2
    assert result.total == 2
    assert result.offset == 0
    assert result.limit == 50
    
    # Check first commit
    assert result.commits[0].commit_id == 'abc123'
    assert result.commits[0].parent_commit_id == 'parent123'
    assert result.commits[0].message == 'Initial commit'
    assert result.commits[0].author_name == 'user001'
    assert result.commits[0].row_count == 100
    
    # Check second commit
    assert result.commits[1].commit_id == 'parent123'
    assert result.commits[1].parent_commit_id is None
    assert result.commits[1].author_name == 'user002'


@pytest.mark.asyncio
async def test_get_commit_history_with_pagination(mock_uow):
    """Test commit history with pagination."""
    # Arrange
    dataset_id = 1
    mock_uow.commits.get_commit_history = AsyncMock(return_value=[])
    mock_uow.commits.count_commits_for_dataset = AsyncMock(return_value=100)
    
    handler = GetCommitHistoryHandler(mock_uow)
    
    # Act
    result = await handler.handle(dataset_id, offset=20, limit=10)
    
    # Assert
    mock_uow.commits.get_commit_history.assert_called_once_with(
        dataset_id=dataset_id,
        offset=20,
        limit=10
    )
    assert result.total == 100
    assert result.offset == 20
    assert result.limit == 10


@pytest.mark.asyncio
async def test_get_commit_history_unknown_user(mock_uow):
    """Test handling of unknown user in commit history."""
    # Arrange
    dataset_id = 1
    test_commit = {
        'commit_id': 'abc123',
        'parent_commit_id': None,
        'message': 'Commit by deleted user',
        'author_id': 999,
        'created_at': datetime.now(),
        'row_count': 10
    }
    
    mock_uow.commits.get_commit_history = AsyncMock(return_value=[test_commit])
    mock_uow.commits.count_commits_for_dataset = AsyncMock(return_value=1)
    mock_uow.users.get_by_id = AsyncMock(return_value=None)
    
    handler = GetCommitHistoryHandler(mock_uow)
    
    # Act
    result = await handler.handle(dataset_id)
    
    # Assert
    assert len(result.commits) == 1
    assert result.commits[0].author_name == 'Unknown'


@pytest.mark.asyncio
async def test_get_commit_history_empty(mock_uow):
    """Test empty commit history."""
    # Arrange
    dataset_id = 1
    mock_uow.commits.get_commit_history = AsyncMock(return_value=[])
    mock_uow.commits.count_commits_for_dataset = AsyncMock(return_value=0)
    
    handler = GetCommitHistoryHandler(mock_uow)
    
    # Act
    result = await handler.handle(dataset_id)
    
    # Assert
    assert len(result.commits) == 0
    assert result.total == 0