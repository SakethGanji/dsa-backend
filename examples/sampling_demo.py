"""
Demo script for SQL-based sampling functionality.

This script demonstrates various sampling methods and their usage patterns.
"""

import asyncio
import json
from datetime import datetime

from src.core.database import DatabasePool
from src.core.services.sampling_service import PostgresSamplingService, SamplingJobService
from src.core.abstractions.services import SamplingMethod, SampleConfig
from src.core.infrastructure.postgres.table_reader import PostgresTableReader


async def demo_direct_sampling(db_pool, commit_id: str, table_key: str = 'primary'):
    """Demonstrate direct sampling using PostgresSamplingService."""
    print("\n=== Direct Sampling Demo ===\n")
    
    async with db_pool.acquire() as conn:
        table_reader = PostgresTableReader(conn)
        sampling_service = PostgresSamplingService(db_pool)
        
        # 1. Random Sampling
        print("1. Random Sampling (100 rows with seed)")
        config = SampleConfig(
            method=SamplingMethod.RANDOM,
            sample_size=100,
            random_seed=42
        )
        result = await sampling_service.sample(table_reader, commit_id, table_key, config)
        print(f"   - Sampled {result.sample_size} rows")
        print(f"   - First 3 rows: {result.sampled_data[:3]}")
        
        # 2. Stratified Sampling
        print("\n2. Stratified Sampling by region")
        config = SampleConfig(
            method=SamplingMethod.STRATIFIED,
            sample_size=200,
            stratify_columns=['region'],
            proportional=True,
            random_seed=42
        )
        result = await sampling_service.sample(table_reader, commit_id, table_key, config)
        print(f"   - Sampled {result.sample_size} rows")
        print(f"   - Strata distribution: {result.strata_counts}")
        
        # 3. Cluster Sampling
        print("\n3. Cluster Sampling by department")
        config = SampleConfig(
            method=SamplingMethod.CLUSTER,
            sample_size=150,
            cluster_column='department_id',
            num_clusters=3,
            random_seed=42
        )
        result = await sampling_service.sample(table_reader, commit_id, table_key, config)
        print(f"   - Sampled {result.sample_size} rows")
        print(f"   - Selected clusters: {result.selected_clusters}")
        
        # 4. Multi-round Sampling
        print("\n4. Multi-round Sampling")
        config = SampleConfig(
            method=SamplingMethod.MULTI_ROUND,
            sample_size=300,  # Total across all rounds
            round_configs=[
                SampleConfig(
                    method=SamplingMethod.RANDOM,
                    sample_size=100,
                    random_seed=1
                ),
                SampleConfig(
                    method=SamplingMethod.STRATIFIED,
                    sample_size=100,
                    stratify_columns=['category'],
                    random_seed=2
                ),
                SampleConfig(
                    method=SamplingMethod.CLUSTER,
                    sample_size=100,
                    cluster_column='region',
                    num_clusters=2,
                    random_seed=3
                )
            ]
        )
        result = await sampling_service.sample(table_reader, commit_id, table_key, config)
        print(f"   - Total sampled: {result.sample_size} rows")
        print(f"   - Rounds: {len(result.round_results)}")
        for i, round_result in enumerate(result.round_results):
            print(f"     Round {i+1}: {round_result.method_used.value} - {round_result.sample_size} rows")


async def demo_job_based_sampling(db_pool, dataset_id: int, commit_id: str):
    """Demonstrate job-based sampling for async processing."""
    print("\n=== Job-Based Sampling Demo ===\n")
    
    job_service = SamplingJobService(db_pool)
    
    # Create a complex sampling job
    sampling_config = {
        'table_key': 'primary',
        'create_output_commit': True,
        'commit_message': 'Sampled data for analysis',
        'rounds': [
            {
                'method': 'stratified',
                'parameters': {
                    'sample_size': 500,
                    'strata_columns': ['region', 'category'],
                    'min_per_stratum': 20,
                    'seed': 42,
                    'filters': {
                        'conditions': [
                            {
                                'column': 'value',
                                'operator': '>=',
                                'value': 1000
                            },
                            {
                                'column': 'date',
                                'operator': '>=',
                                'value': '2024-01-15'
                            }
                        ],
                        'logic': 'AND'
                    }
                }
            },
            {
                'method': 'random',
                'parameters': {
                    'sample_size': 200,
                    'seed': 123,
                    'filters': {
                        'conditions': [
                            {
                                'column': 'region',
                                'operator': 'not_in',
                                'value': ['North']  # Exclude North region
                            }
                        ]
                    }
                }
            }
        ]
    }
    
    # Create the job
    job_id = await job_service.create_sampling_job(
        dataset_id=dataset_id,
        source_commit_id=commit_id,
        user_id=1,
        sampling_config=sampling_config
    )
    
    print(f"Created sampling job: {job_id}")
    
    # Poll for job completion (in production, this would be handled by the worker)
    max_attempts = 30
    for i in range(max_attempts):
        await asyncio.sleep(2)
        status = await job_service.get_job_status(job_id)
        print(f"Job status: {status['status']}")
        
        if status['status'] == 'completed':
            print(f"Job completed successfully!")
            print(f"Output: {json.dumps(status['output_summary'], indent=2)}")
            break
        elif status['status'] == 'failed':
            print(f"Job failed: {status['error_message']}")
            break
    else:
        print("Job did not complete within timeout")


async def demo_column_sampling(db_pool, commit_id: str, table_key: str = 'primary'):
    """Demonstrate column value sampling for analysis."""
    print("\n=== Column Sampling Demo ===\n")
    
    async with db_pool.acquire() as conn:
        table_reader = PostgresTableReader(conn)
        
        # Sample unique values from multiple columns
        columns = ['region', 'category', 'department_id']
        samples = await table_reader.get_column_samples(
            commit_id, table_key, columns, samples_per_column=10
        )
        
        print("Column value samples:")
        for col, values in samples.items():
            print(f"  {col}: {values}")


async def demo_streaming_sampling(db_pool, commit_id: str, table_key: str = 'primary'):
    """Demonstrate streaming sampling for large datasets."""
    print("\n=== Streaming Sampling Demo ===\n")
    
    async with db_pool.acquire() as conn:
        table_reader = PostgresTableReader(conn)
        
        # Stream random samples
        sample_params = {
            'sample_size': 1000,
            'seed': 42
        }
        
        print("Streaming 1000 random samples...")
        row_count = 0
        batch_count = 0
        
        async for row in table_reader.get_table_sample_stream(
            commit_id, table_key, 'random', sample_params
        ):
            row_count += 1
            if row_count % 100 == 0:
                batch_count += 1
                print(f"  Processed batch {batch_count} (100 rows)")
            
            # Process row here - in production, you might write to file or transform
            if row_count >= 1000:
                break
        
        print(f"Total rows streamed: {row_count}")


async def main():
    """Main demo function."""
    # Initialize database
    db_pool = DatabasePool("postgresql://user:password@localhost/dsa")
    await db_pool.initialize()
    
    try:
        # You would replace these with actual values from your database
        # For demo purposes, we'll use placeholder values
        commit_id = "your_commit_id_here"
        dataset_id = 1
        table_key = "primary"
        
        print("SQL-Based Sampling System Demo")
        print("=" * 50)
        
        # Run demos
        await demo_direct_sampling(db_pool, commit_id, table_key)
        await demo_column_sampling(db_pool, commit_id, table_key)
        await demo_streaming_sampling(db_pool, commit_id, table_key)
        await demo_job_based_sampling(db_pool, dataset_id, commit_id)
        
    finally:
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())