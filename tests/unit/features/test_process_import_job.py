"""Unit tests for ProcessImportJobHandler with new abstractions."""

import pytest
from unittest.mock import Mock, AsyncMock, MagicMock, patch
import tempfile
import os
import pandas as pd
from uuid import uuid4

from src.features.jobs.process_import_job import ProcessImportJobHandler
from src.core.abstractions import (
    IUnitOfWork, IJobRepository, ICommitRepository
)
from src.core.abstractions.services import (
    IFileProcessingService, IStatisticsService,
    ParsedData, TableData, TableStatistics, ColumnStatistics
)


class TestProcessImportJobHandler:
    """Test ProcessImportJobHandler with new abstractions."""
    
    @pytest.fixture
    def mock_uow(self):
        """Create mock unit of work."""
        uow = AsyncMock(spec=IUnitOfWork)
        uow.begin = AsyncMock()
        uow.commit = AsyncMock()
        uow.rollback = AsyncMock()
        return uow
    
    @pytest.fixture
    def mock_job_repo(self):
        """Create mock job repository."""
        repo = AsyncMock(spec=IJobRepository)
        return repo
    
    @pytest.fixture
    def mock_commit_repo(self):
        """Create mock commit repository."""
        repo = AsyncMock(spec=ICommitRepository)
        return repo
    
    @pytest.fixture
    def mock_parser_factory(self):
        """Create mock parser factory."""
        factory = Mock(spec=IFileProcessingService)
        return factory
    
    @pytest.fixture
    def mock_stats_calculator(self):
        """Create mock statistics calculator."""
        calc = AsyncMock(spec=IStatisticsService)
        return calc
    
    @pytest.fixture
    def handler(self, mock_uow, mock_job_repo, mock_commit_repo, 
                mock_parser_factory, mock_stats_calculator):
        """Create handler instance with mocks."""
        return ProcessImportJobHandler(
            uow=mock_uow,
            job_repo=mock_job_repo,
            commit_repo=mock_commit_repo,
            parser_factory=mock_parser_factory,
            stats_calculator=mock_stats_calculator
        )
    
    @pytest.fixture
    def sample_job(self):
        """Create sample job data."""
        return {
            'id': uuid4(),
            'status': 'pending',
            'dataset_id': 1,
            'user_id': 1,
            'source_commit_id': 'parent123',
            'run_parameters': {
                'temp_file_path': '/tmp/test_file.csv',
                'filename': 'test.csv',
                'target_ref': 'main',
                'commit_message': 'Import test data'
            }
        }
    
    @pytest.fixture
    def sample_parsed_data(self):
        """Create sample parsed data."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        })
        return ParsedData(
            tables=[TableData(table_key='primary', dataframe=df)],
            file_type='csv',
            filename='test.csv'
        )
    
    @pytest.fixture
    def sample_table_stats(self):
        """Create sample table statistics."""
        return TableStatistics(
            row_count=3,
            column_count=3,
            columns={
                'id': ColumnStatistics(
                    name='id', dtype='integer', null_count=0,
                    null_percentage=0.0, unique_count=3
                ),
                'name': ColumnStatistics(
                    name='name', dtype='string', null_count=0,
                    null_percentage=0.0, unique_count=3
                ),
                'value': ColumnStatistics(
                    name='value', dtype='integer', null_count=0,
                    null_percentage=0.0, unique_count=3
                )
            },
            memory_usage_bytes=1024,
            unique_row_count=3,
            duplicate_row_count=0
        )
    
    @pytest.mark.asyncio
    async def test_handle_success(self, handler, sample_job, sample_parsed_data,
                                  sample_table_stats, mock_job_repo, mock_commit_repo,
                                  mock_parser_factory, mock_stats_calculator):
        """Test successful job processing."""
        job_id = sample_job['id']
        
        # Setup mocks
        mock_job_repo.get_job_by_id.return_value = sample_job
        mock_commit_repo.get_current_commit_for_ref.return_value = 'parent123'
        
        # Setup parser mock
        mock_parser = AsyncMock()
        mock_parser.parse.return_value = sample_parsed_data
        mock_parser_factory.get_parser.return_value = mock_parser
        
        # Setup stats calculator mock
        mock_stats_calculator.calculate_table_statistics.return_value = sample_table_stats
        mock_stats_calculator.get_summary_dict.return_value = {
            'row_count': 3,
            'column_count': 3
        }
        
        # Setup commit creation
        mock_commit_repo.create_commit_and_manifest.return_value = 'new_commit_123'
        mock_commit_repo.update_ref_atomically.return_value = True
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data")
            temp_path = f.name
        
        # Update job parameters with real temp file
        sample_job['run_parameters']['temp_file_path'] = temp_path
        
        try:
            # Execute
            await handler.handle(job_id)
            
            # Verify job status updates
            assert mock_job_repo.update_job_status.call_count == 2
            # First call: mark as running
            mock_job_repo.update_job_status.assert_any_call(job_id, 'running')
            # Second call: mark as completed
            final_call = mock_job_repo.update_job_status.call_args_list[-1]
            assert final_call[0][0] == job_id
            assert final_call[0][1] == 'completed'
            assert 'output_summary' in final_call[1]
            assert final_call[1]['output_summary']['new_commit_id'] == 'new_commit_123'
            
            # Verify parser was used
            mock_parser_factory.get_parser.assert_called_once_with('test.csv')
            mock_parser.parse.assert_called_once_with(temp_path, 'test.csv')
            
            # Verify statistics were calculated
            mock_stats_calculator.calculate_table_statistics.assert_called_once()
            
            # Verify commit was created
            mock_commit_repo.add_rows_if_not_exist.assert_called_once()
            mock_commit_repo.create_commit_and_manifest.assert_called_once()
            mock_commit_repo.create_commit_schema.assert_called_once()
            mock_commit_repo.create_commit_statistics.assert_called_once()
            
            # Verify temp file was cleaned up
            assert not os.path.exists(temp_path)
            
        finally:
            # Clean up if test fails
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    @pytest.mark.asyncio
    async def test_handle_job_not_found(self, handler, mock_job_repo):
        """Test handling when job is not found."""
        job_id = uuid4()
        mock_job_repo.get_job_by_id.return_value = None
        
        with pytest.raises(ValueError, match="Job .* not found"):
            await handler.handle(job_id)
    
    @pytest.mark.asyncio
    async def test_handle_job_already_processed(self, handler, sample_job, mock_job_repo):
        """Test handling when job is already processed."""
        sample_job['status'] = 'completed'
        mock_job_repo.get_job_by_id.return_value = sample_job
        
        # Should return without processing
        await handler.handle(sample_job['id'])
        
        # Verify no processing occurred
        mock_job_repo.update_job_status.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_handle_optimistic_lock_failure(self, handler, sample_job,
                                                  mock_job_repo, mock_commit_repo):
        """Test handling when optimistic locking fails."""
        job_id = sample_job['id']
        
        # Setup mocks
        mock_job_repo.get_job_by_id.return_value = sample_job
        # Current commit differs from source commit
        mock_commit_repo.get_current_commit_for_ref.return_value = 'different_commit'
        
        # Create temp file
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name
        sample_job['run_parameters']['temp_file_path'] = temp_path
        
        try:
            # Execute - should fail
            await handler.handle(job_id)
            
            # Verify job marked as failed
            final_call = mock_job_repo.update_job_status.call_args_list[-1]
            assert final_call[0][1] == 'failed'
            assert 'Conflict' in final_call[1]['error_message']
            
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    @pytest.mark.asyncio
    async def test_handle_parser_error(self, handler, sample_job, mock_job_repo,
                                       mock_commit_repo, mock_parser_factory):
        """Test handling when file parsing fails."""
        job_id = sample_job['id']
        
        # Setup mocks
        mock_job_repo.get_job_by_id.return_value = sample_job
        mock_commit_repo.get_current_commit_for_ref.return_value = 'parent123'
        
        # Parser fails
        mock_parser = AsyncMock()
        mock_parser.parse.side_effect = Exception("Invalid file format")
        mock_parser_factory.get_parser.return_value = mock_parser
        
        # Create temp file
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name
        sample_job['run_parameters']['temp_file_path'] = temp_path
        
        try:
            await handler.handle(job_id)
            
            # Verify job marked as failed
            final_call = mock_job_repo.update_job_status.call_args_list[-1]
            assert final_call[0][1] == 'failed'
            assert 'Invalid file format' in final_call[1]['error_message']
            
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    @pytest.mark.asyncio
    async def test_parse_file_integration(self, handler, mock_parser_factory,
                                          mock_stats_calculator, sample_parsed_data,
                                          sample_table_stats):
        """Test _parse_file method integration."""
        # Setup mocks
        mock_parser = AsyncMock()
        mock_parser.parse.return_value = sample_parsed_data
        mock_parser_factory.get_parser.return_value = mock_parser
        
        mock_stats_calculator.calculate_table_statistics.return_value = sample_table_stats
        mock_stats_calculator.get_summary_dict.return_value = {
            'row_count': 3,
            'column_count': 3
        }
        
        # Execute
        rows, manifest, schema, stats = await handler._parse_file('/tmp/test.csv', 'test.csv')
        
        # Verify results
        assert len(rows) == 3  # 3 unique rows
        assert len(manifest) == 3  # 3 logical row IDs
        assert 'primary' in schema
        assert 'primary' in stats
        
        # Check manifest format
        for logical_id, _ in manifest:
            assert logical_id.startswith('primary:')
        
        # Check schema structure
        assert 'columns' in schema['primary']
        assert 'row_count' in schema['primary']
        assert schema['primary']['row_count'] == 3