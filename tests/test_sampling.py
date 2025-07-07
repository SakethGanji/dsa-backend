"""Tests for SQL-based sampling implementation."""

import pytest
import asyncio
import json
from uuid import UUID
from datetime import datetime

from src.core.database import DatabasePool
from src.core.services.sampling_service import PostgresSamplingService, SamplingJobService
from src.core.abstractions.services import SamplingMethod, SampleConfig
from src.core.infrastructure.postgres.table_reader import PostgresTableReader
from src.workers.sampling_executor import SamplingJobExecutor


@pytest.fixture
async def db_pool():
    """Create a test database pool."""
    # Use test database URL
    pool = DatabasePool("postgresql://user:password@localhost/dsa_test")
    await pool.initialize()
    yield pool
    await pool.close()


@pytest.fixture
async def sample_commit_data(db_pool):
    """Create sample data for testing."""
    async with db_pool.acquire() as conn:
        # Create a test dataset
        dataset_id = await conn.fetchval("""
            INSERT INTO dsa_core.datasets (name, description, created_by)
            VALUES ('Test Dataset', 'Dataset for sampling tests', 1)
            RETURNING id
        """)
        
        # Create test rows with different characteristics
        rows_data = []
        for i in range(1000):
            row_data = {
                'id': i,
                'region': ['North', 'South', 'East', 'West'][i % 4],
                'category': ['A', 'B', 'C'][i % 3],
                'value': i * 10,
                'date': f'2024-01-{(i % 28) + 1:02d}',
                'department_id': f'DEPT_{i % 10}'
            }
            row_json = json.dumps(row_data)
            row_hash = hashlib.sha256(row_json.encode()).hexdigest()
            rows_data.append((row_hash, row_json))
        
        # Insert unique rows
        await conn.executemany("""
            INSERT INTO dsa_core.rows (row_hash, data)
            VALUES ($1, $2::jsonb)
            ON CONFLICT DO NOTHING
        """, rows_data)
        
        # Create a commit
        commit_id = hashlib.sha256(f"test_commit_{datetime.utcnow()}".encode()).hexdigest()
        await conn.execute("""
            INSERT INTO dsa_core.commits (commit_id, dataset_id, message, author_id)
            VALUES ($1, $2, 'Test commit for sampling', 1)
        """, commit_id, dataset_id)
        
        # Create manifest
        manifest_data = [(commit_id, f"primary:{i}", rows_data[i][0]) for i in range(1000)]
        await conn.executemany("""
            INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
            VALUES ($1, $2, $3)
        """, manifest_data)
        
        # Create schema
        schema = {
            'primary': {
                'columns': [
                    {'name': 'id', 'type': 'integer'},
                    {'name': 'region', 'type': 'text'},
                    {'name': 'category', 'type': 'text'},
                    {'name': 'value', 'type': 'integer'},
                    {'name': 'date', 'type': 'date'},
                    {'name': 'department_id', 'type': 'text'}
                ]
            }
        }
        await conn.execute("""
            INSERT INTO dsa_core.commit_schemas (commit_id, schema_definition)
            VALUES ($1, $2)
        """, commit_id, json.dumps(schema))
        
        return {
            'dataset_id': dataset_id,
            'commit_id': commit_id,
            'total_rows': 1000
        }


class TestPostgresSamplingService:
    """Test PostgresSamplingService functionality."""
    
    @pytest.mark.asyncio
    async def test_random_sampling(self, db_pool, sample_commit_data):
        """Test random sampling method."""
        async with db_pool.acquire() as conn:
            table_reader = PostgresTableReader(conn)
            sampling_service = PostgresSamplingService(db_pool)
            
            config = SampleConfig(
                method=SamplingMethod.RANDOM,
                sample_size=100,
                random_seed=42
            )
            
            result = await sampling_service.sample(
                table_reader,
                sample_commit_data['commit_id'],
                'primary',
                config
            )
            
            assert result.sample_size == 100
            assert result.method_used == SamplingMethod.RANDOM
            assert len(result.sampled_data) == 100
            
            # Verify deterministic behavior with seed
            result2 = await sampling_service.sample(
                table_reader,
                sample_commit_data['commit_id'],
                'primary',
                config
            )
            
            # Same seed should produce same results
            assert [r['id'] for r in result.sampled_data] == [r['id'] for r in result2.sampled_data]
    
    @pytest.mark.asyncio
    async def test_stratified_sampling(self, db_pool, sample_commit_data):
        """Test stratified sampling method."""
        async with db_pool.acquire() as conn:
            table_reader = PostgresTableReader(conn)
            sampling_service = PostgresSamplingService(db_pool)
            
            config = SampleConfig(
                method=SamplingMethod.STRATIFIED,
                sample_size=200,
                stratify_columns=['region'],
                proportional=True,
                random_seed=42
            )
            
            result = await sampling_service.sample(
                table_reader,
                sample_commit_data['commit_id'],
                'primary',
                config
            )
            
            assert result.method_used == SamplingMethod.STRATIFIED
            assert result.strata_counts is not None
            
            # Verify all regions are represented
            regions_in_sample = set(row['region'] for row in result.sampled_data)
            assert regions_in_sample == {'North', 'South', 'East', 'West'}
            
            # Verify proportional sampling
            total_samples = len(result.sampled_data)
            for stratum, count in result.strata_counts.items():
                # Each region should have roughly 25% of samples
                assert 0.20 <= count / total_samples <= 0.30
    
    @pytest.mark.asyncio
    async def test_systematic_sampling(self, db_pool, sample_commit_data):
        """Test systematic sampling method."""
        async with db_pool.acquire() as conn:
            table_reader = PostgresTableReader(conn)
            sampling_service = PostgresSamplingService(db_pool)
            
            config = SampleConfig(
                method=SamplingMethod.SYSTEMATIC,
                sample_size=50  # Will calculate interval internally
            )
            
            result = await sampling_service.sample(
                table_reader,
                sample_commit_data['commit_id'],
                'primary',
                config
            )
            
            assert result.method_used == SamplingMethod.SYSTEMATIC
            # Systematic sampling should produce evenly spaced samples
            assert len(result.sampled_data) > 0
    
    @pytest.mark.asyncio
    async def test_cluster_sampling(self, db_pool, sample_commit_data):
        """Test cluster sampling method."""
        async with db_pool.acquire() as conn:
            table_reader = PostgresTableReader(conn)
            sampling_service = PostgresSamplingService(db_pool)
            
            config = SampleConfig(
                method=SamplingMethod.CLUSTER,
                sample_size=100,
                cluster_column='department_id',
                num_clusters=3,
                random_seed=42
            )
            
            result = await sampling_service.sample(
                table_reader,
                sample_commit_data['commit_id'],
                'primary',
                config
            )
            
            assert result.method_used == SamplingMethod.CLUSTER
            assert result.selected_clusters is not None
            assert len(result.selected_clusters) <= 3
            
            # Verify only selected clusters are in the sample
            departments_in_sample = set(row['department_id'] for row in result.sampled_data)
            assert departments_in_sample.issubset(set(result.selected_clusters))


class TestSamplingJobExecutor:
    """Test SamplingJobExecutor functionality."""
    
    @pytest.mark.asyncio
    async def test_random_sampling_job(self, db_pool, sample_commit_data):
        """Test random sampling job execution."""
        executor = SamplingJobExecutor()
        
        parameters = {
            'source_commit_id': sample_commit_data['commit_id'],
            'dataset_id': sample_commit_data['dataset_id'],
            'table_key': 'primary',
            'create_output_commit': True,
            'user_id': 1,
            'rounds': [{
                'method': 'random',
                'parameters': {
                    'sample_size': 100,
                    'seed': 42
                }
            }]
        }
        
        result = await executor.execute('test_job_1', parameters, db_pool)
        
        assert result['status'] == 'completed'
        assert result['total_sampled'] == 100
        assert result['output_commit_id'] is not None
        assert len(result['round_results']) == 1
        assert result['round_results'][0]['rows_sampled'] == 100
    
    @pytest.mark.asyncio
    async def test_multi_round_sampling_job(self, db_pool, sample_commit_data):
        """Test multi-round sampling job execution."""
        executor = SamplingJobExecutor()
        
        parameters = {
            'source_commit_id': sample_commit_data['commit_id'],
            'dataset_id': sample_commit_data['dataset_id'],
            'table_key': 'primary',
            'create_output_commit': True,
            'user_id': 1,
            'rounds': [
                {
                    'method': 'random',
                    'parameters': {
                        'sample_size': 50,
                        'seed': 1
                    }
                },
                {
                    'method': 'stratified',
                    'parameters': {
                        'sample_size': 50,
                        'strata_columns': ['region'],
                        'min_per_stratum': 10,
                        'seed': 2
                    }
                }
            ]
        }
        
        result = await executor.execute('test_job_2', parameters, db_pool)
        
        assert result['status'] == 'completed'
        assert result['total_sampled'] == 100  # 50 + 50
        assert len(result['round_results']) == 2
        assert result['round_results'][0]['method'] == 'random'
        assert result['round_results'][1]['method'] == 'stratified'
    
    @pytest.mark.asyncio
    async def test_sampling_with_filters(self, db_pool, sample_commit_data):
        """Test sampling with dynamic filters."""
        executor = SamplingJobExecutor()
        
        parameters = {
            'source_commit_id': sample_commit_data['commit_id'],
            'dataset_id': sample_commit_data['dataset_id'],
            'table_key': 'primary',
            'create_output_commit': False,
            'rounds': [{
                'method': 'random',
                'parameters': {
                    'sample_size': 50,
                    'filters': {
                        'conditions': [
                            {
                                'column': 'region',
                                'operator': 'in',
                                'value': ['North', 'South']
                            },
                            {
                                'column': 'value',
                                'operator': '>',
                                'value': 5000
                            }
                        ],
                        'logic': 'AND'
                    }
                }
            }]
        }
        
        result = await executor.execute('test_job_3', parameters, db_pool)
        
        assert result['status'] == 'completed'
        # Should have filtered results
        assert result['total_sampled'] <= 50
    
    @pytest.mark.asyncio
    async def test_invalid_column_security(self, db_pool, sample_commit_data):
        """Test security validation for column names."""
        executor = SamplingJobExecutor()
        
        parameters = {
            'source_commit_id': sample_commit_data['commit_id'],
            'dataset_id': sample_commit_data['dataset_id'],
            'table_key': 'primary',
            'rounds': [{
                'method': 'stratified',
                'parameters': {
                    'sample_size': 50,
                    'strata_columns': ['region; DROP TABLE users;--']  # SQL injection attempt
                }
            }]
        }
        
        with pytest.raises(ValueError, match="Invalid column name"):
            await executor.execute('test_job_4', parameters, db_pool)


class TestSamplingJobService:
    """Test SamplingJobService functionality."""
    
    @pytest.mark.asyncio
    async def test_create_sampling_job(self, db_pool, sample_commit_data):
        """Test creating a sampling job."""
        job_service = SamplingJobService(db_pool)
        
        sampling_config = {
            'rounds': [{
                'method': 'random',
                'parameters': {
                    'sample_size': 100,
                    'seed': 42
                }
            }]
        }
        
        job_id = await job_service.create_sampling_job(
            dataset_id=sample_commit_data['dataset_id'],
            source_commit_id=sample_commit_data['commit_id'],
            user_id=1,
            sampling_config=sampling_config
        )
        
        assert job_id is not None
        assert UUID(job_id)  # Valid UUID
        
        # Check job status
        status = await job_service.get_job_status(job_id)
        assert status['job_id'] == job_id
        assert status['status'] == 'pending'
        assert status['output_summary'] is None


# Required imports
import hashlib