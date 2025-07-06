"""Unit tests for CheckoutCommitHandler."""

import pytest
from unittest.mock import Mock, AsyncMock

from src.features.versioning.checkout_commit import CheckoutCommitHandler
from src.models.pydantic_models import CheckoutResponse


@pytest.fixture
def mock_uow():
    """Create a mock unit of work."""
    uow = Mock()
    
    # Mock repositories need to be available both on uow and on the context manager return
    commits_mock = Mock()
    
    # Set up the context manager
    uow.__aenter__ = AsyncMock(return_value=uow)
    uow.__aexit__ = AsyncMock(return_value=None)
    
    # Make commits available on the uow instance
    uow.commits = commits_mock
    
    return uow


@pytest.mark.asyncio
async def test_checkout_commit_success(mock_uow):
    """Test successful checkout of a commit."""
    # Arrange
    dataset_id = 1
    commit_id = "abc123"
    
    mock_commit = {
        'commit_id': commit_id,
        'dataset_id': dataset_id,
        'message': 'Test commit'
    }
    
    mock_data_rows = [
        {
            'logical_row_id': 'Sheet1:0',
            'data': {'id': 1, 'name': 'Alice', 'value': 100}
        },
        {
            'logical_row_id': 'Sheet1:1',
            'data': {'id': 2, 'name': 'Bob', 'value': 200}
        }
    ]
    
    mock_uow.commits.get_commit_by_id = AsyncMock(return_value=mock_commit)
    mock_uow.commits.get_commit_data = AsyncMock(return_value=mock_data_rows)
    mock_uow.commits.count_commit_rows = AsyncMock(return_value=2)
    
    handler = CheckoutCommitHandler(mock_uow)
    
    # Act
    result = await handler.handle(dataset_id, commit_id)
    
    # Assert
    assert isinstance(result, CheckoutResponse)
    assert result.commit_id == commit_id
    assert len(result.data) == 2
    assert result.total_rows == 2
    assert result.offset == 0
    assert result.limit == 100
    
    # Check data content
    assert result.data[0]['id'] == 1
    assert result.data[0]['name'] == 'Alice'
    assert result.data[0]['_logical_row_id'] == 'Sheet1:0'
    assert result.data[1]['id'] == 2
    assert result.data[1]['name'] == 'Bob'
    assert result.data[1]['_logical_row_id'] == 'Sheet1:1'


@pytest.mark.asyncio
async def test_checkout_commit_with_table_filter(mock_uow):
    """Test checkout with table key filter."""
    # Arrange
    dataset_id = 1
    commit_id = "abc123"
    table_key = "Revenue"
    
    mock_commit = {
        'commit_id': commit_id,
        'dataset_id': dataset_id
    }
    
    mock_data_rows = [
        {
            'logical_row_id': 'Revenue:0',
            'data': {'month': 'Jan', 'amount': 1000}
        }
    ]
    
    mock_uow.commits.get_commit_by_id = AsyncMock(return_value=mock_commit)
    mock_uow.commits.get_commit_data = AsyncMock(return_value=mock_data_rows)
    mock_uow.commits.count_commit_rows = AsyncMock(return_value=1)
    
    handler = CheckoutCommitHandler(mock_uow)
    
    # Act
    result = await handler.handle(dataset_id, commit_id, table_key=table_key)
    
    # Assert
    mock_uow.commits.get_commit_data.assert_called_once_with(
        commit_id=commit_id,
        table_key=table_key,
        offset=0,
        limit=100
    )
    mock_uow.commits.count_commit_rows.assert_called_once_with(commit_id, table_key)
    assert len(result.data) == 1
    assert result.data[0]['month'] == 'Jan'


@pytest.mark.asyncio
async def test_checkout_commit_not_found(mock_uow):
    """Test checkout of non-existent commit."""
    # Arrange
    dataset_id = 1
    commit_id = "nonexistent"
    
    mock_uow.commits.get_commit_by_id = AsyncMock(return_value=None)
    mock_uow.commits.get_commit_data = AsyncMock(return_value=[])
    mock_uow.commits.count_commit_rows = AsyncMock(return_value=0)
    
    handler = CheckoutCommitHandler(mock_uow)
    
    # Act & Assert
    with pytest.raises(ValueError, match="Commit not found for this dataset"):
        await handler.handle(dataset_id, commit_id)


@pytest.mark.asyncio
async def test_checkout_commit_wrong_dataset(mock_uow):
    """Test checkout of commit from different dataset."""
    # Arrange
    dataset_id = 1
    commit_id = "abc123"
    
    mock_commit = {
        'commit_id': commit_id,
        'dataset_id': 2  # Different dataset
    }
    
    mock_uow.commits.get_commit_by_id = AsyncMock(return_value=mock_commit)
    mock_uow.commits.get_commit_data = AsyncMock(return_value=[])
    mock_uow.commits.count_commit_rows = AsyncMock(return_value=0)
    
    handler = CheckoutCommitHandler(mock_uow)
    
    # Act & Assert
    with pytest.raises(ValueError, match="Commit not found for this dataset"):
        await handler.handle(dataset_id, commit_id)


@pytest.mark.asyncio
async def test_checkout_commit_with_pagination(mock_uow):
    """Test checkout with pagination parameters."""
    # Arrange
    dataset_id = 1
    commit_id = "abc123"
    offset = 10
    limit = 5
    
    mock_commit = {
        'commit_id': commit_id,
        'dataset_id': dataset_id
    }
    
    mock_uow.commits.get_commit_by_id = AsyncMock(return_value=mock_commit)
    mock_uow.commits.get_commit_data = AsyncMock(return_value=[])
    mock_uow.commits.count_commit_rows = AsyncMock(return_value=100)
    
    handler = CheckoutCommitHandler(mock_uow)
    
    # Act
    result = await handler.handle(dataset_id, commit_id, offset=offset, limit=limit)
    
    # Assert
    mock_uow.commits.get_commit_data.assert_called_once_with(
        commit_id=commit_id,
        table_key=None,
        offset=offset,
        limit=limit
    )
    assert result.offset == offset
    assert result.limit == limit
    assert result.total_rows == 100