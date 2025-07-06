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
    table_reader_mock = Mock()
    
    # Set up the context manager
    uow.__aenter__ = AsyncMock(return_value=uow)
    uow.__aexit__ = AsyncMock(return_value=None)
    
    # Make commits and table_reader available on the uow instance
    uow.commits = commits_mock
    uow.table_reader = table_reader_mock
    
    return uow


@pytest.fixture
def mock_table_reader():
    """Create a mock table reader."""
    return Mock()


@pytest.mark.asyncio
async def test_checkout_commit_success(mock_uow, mock_table_reader):
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
            '_logical_row_id': 'Sheet1:0',
            'id': 1,
            'name': 'Alice',
            'value': 100
        },
        {
            '_logical_row_id': 'Sheet1:1',
            'id': 2,
            'name': 'Bob',
            'value': 200
        }
    ]
    
    mock_uow.commits.get_commit_by_id = AsyncMock(return_value=mock_commit)
    mock_table_reader.list_table_keys = AsyncMock(return_value=['primary'])
    mock_table_reader.get_table_data = AsyncMock(return_value=mock_data_rows)
    mock_table_reader.count_table_rows = AsyncMock(return_value=2)
    
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
    
    # Verify table reader was called correctly
    mock_table_reader.list_table_keys.assert_called_once_with(commit_id)
    mock_table_reader.get_table_data.assert_called_once_with(
        commit_id=commit_id,
        table_key='primary',
        offset=0,
        limit=100
    )


@pytest.mark.asyncio
async def test_checkout_commit_with_table_filter(mock_uow, mock_table_reader):
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
            '_logical_row_id': 'Revenue:0',
            'month': 'Jan',
            'amount': 1000
        }
    ]
    
    mock_uow.commits.get_commit_by_id = AsyncMock(return_value=mock_commit)
    mock_table_reader.get_table_data = AsyncMock(return_value=mock_data_rows)
    mock_table_reader.count_table_rows = AsyncMock(return_value=1)
    
    handler = CheckoutCommitHandler(mock_uow)
    
    # Act
    result = await handler.handle(dataset_id, commit_id, table_key=table_key)
    
    # Assert
    mock_table_reader.get_table_data.assert_called_once_with(
        commit_id=commit_id,
        table_key=table_key,
        offset=0,
        limit=100
    )
    mock_table_reader.count_table_rows.assert_called_once_with(commit_id, table_key)
    assert len(result.data) == 1
    assert result.data[0]['month'] == 'Jan'


@pytest.mark.asyncio
async def test_checkout_commit_not_found(mock_uow, mock_table_reader):
    """Test checkout of non-existent commit."""
    # Arrange
    dataset_id = 1
    commit_id = "nonexistent"
    
    mock_uow.commits.get_commit_by_id = AsyncMock(return_value=None)
    
    handler = CheckoutCommitHandler(mock_uow)
    
    # Act & Assert
    with pytest.raises(ValueError, match="Commit not found for this dataset"):
        await handler.handle(dataset_id, commit_id)


@pytest.mark.asyncio
async def test_checkout_commit_wrong_dataset(mock_uow, mock_table_reader):
    """Test checkout of commit from different dataset."""
    # Arrange
    dataset_id = 1
    commit_id = "abc123"
    
    mock_commit = {
        'commit_id': commit_id,
        'dataset_id': 2  # Different dataset
    }
    
    mock_uow.commits.get_commit_by_id = AsyncMock(return_value=mock_commit)
    
    handler = CheckoutCommitHandler(mock_uow)
    
    # Act & Assert
    with pytest.raises(ValueError, match="Commit not found for this dataset"):
        await handler.handle(dataset_id, commit_id)


@pytest.mark.asyncio
async def test_checkout_commit_with_pagination(mock_uow, mock_table_reader):
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
    mock_table_reader.list_table_keys = AsyncMock(return_value=['primary'])
    mock_table_reader.get_table_data = AsyncMock(return_value=[])
    mock_table_reader.count_table_rows = AsyncMock(return_value=100)
    
    handler = CheckoutCommitHandler(mock_uow)
    
    # Act
    result = await handler.handle(dataset_id, commit_id, offset=offset, limit=limit)
    
    # Assert
    mock_table_reader.get_table_data.assert_called_once_with(
        commit_id=commit_id,
        table_key='primary',
        offset=offset,
        limit=limit
    )
    assert result.offset == offset
    assert result.limit == limit
    assert result.total_rows == 100