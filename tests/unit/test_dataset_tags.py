"""Unit tests for dataset tag functionality."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from src.core.infrastructure.postgres.dataset_repo import PostgresDatasetRepository


class TestDatasetTagMethods:
    """Test PostgresDatasetRepository tag methods."""
    
    @pytest.mark.asyncio
    async def test_add_dataset_tags_empty_list(self):
        """Test adding empty tag list does nothing."""
        mock_conn = AsyncMock()
        repo = PostgresDatasetRepository(mock_conn)
        
        await repo.add_dataset_tags(1, [])
        
        # Should not execute any queries
        mock_conn.execute.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_add_dataset_tags_single_tag(self):
        """Test adding a single tag."""
        mock_conn = AsyncMock()
        repo = PostgresDatasetRepository(mock_conn)
        
        await repo.add_dataset_tags(1, ["financial"])
        
        # Should execute 2 queries: insert tag, link to dataset
        assert mock_conn.execute.call_count == 2
        
        # Check first call (insert tag)
        first_call = mock_conn.execute.call_args_list[0]
        assert "INSERT INTO dsa_core.tags" in first_call[0][0]
        assert first_call[0][1] == "financial"
        
        # Check second call (link tag to dataset)
        second_call = mock_conn.execute.call_args_list[1]
        assert "INSERT INTO dsa_core.dataset_tags" in second_call[0][0]
        assert second_call[0][1] == 1  # dataset_id
        assert second_call[0][2] == ["financial"]  # tags array
    
    @pytest.mark.asyncio
    async def test_add_dataset_tags_multiple_tags(self):
        """Test adding multiple tags."""
        mock_conn = AsyncMock()
        repo = PostgresDatasetRepository(mock_conn)
        
        tags = ["financial", "quarterly", "2024"]
        await repo.add_dataset_tags(1, tags)
        
        # Should execute 4 queries: 3 tag inserts + 1 link
        assert mock_conn.execute.call_count == 4
        
        # Check that all tags were inserted
        tag_insert_calls = [call for call in mock_conn.execute.call_args_list[:3]]
        inserted_tags = [call[0][1] for call in tag_insert_calls]
        assert set(inserted_tags) == set(tags)
        
        # Check link query
        link_call = mock_conn.execute.call_args_list[3]
        assert link_call[0][1] == 1  # dataset_id
        assert link_call[0][2] == tags  # all tags
    
    @pytest.mark.asyncio
    async def test_get_dataset_tags(self):
        """Test getting tags for a dataset."""
        mock_conn = AsyncMock()
        
        # Mock the fetch response
        mock_rows = [
            {"tag_name": "financial"},
            {"tag_name": "quarterly"},
            {"tag_name": "2024"}
        ]
        mock_conn.fetch.return_value = mock_rows
        
        repo = PostgresDatasetRepository(mock_conn)
        
        tags = await repo.get_dataset_tags(1)
        
        # Check query
        mock_conn.fetch.assert_called_once()
        query = mock_conn.fetch.call_args[0][0]
        assert "SELECT t.tag_name" in query
        assert "FROM dsa_core.tags t" in query
        assert "INNER JOIN dsa_core.dataset_tags dt" in query
        assert mock_conn.fetch.call_args[0][1] == 1  # dataset_id
        
        # Check result
        assert tags == ["financial", "quarterly", "2024"]
    
    @pytest.mark.asyncio
    async def test_get_dataset_tags_empty(self):
        """Test getting tags for dataset with no tags."""
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []
        
        repo = PostgresDatasetRepository(mock_conn)
        
        tags = await repo.get_dataset_tags(1)
        
        assert tags == []