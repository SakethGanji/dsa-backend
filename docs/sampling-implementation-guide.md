# SQL-Based Sampling Implementation Guide

## Overview

This guide covers the SQL-based sampling implementation for the DSA platform. The implementation processes data entirely within PostgreSQL for optimal performance and scalability.

## Architecture

### Components

1. **SamplingJobExecutor** (`src/workers/sampling_executor.py`)
   - Executes sampling jobs using SQL queries
   - Supports multiple sampling methods
   - Handles security (column validation, operator whitelisting)
   - Creates output commits with sampled data

2. **PostgresSamplingService** (`src/core/services/sampling_service.py`)
   - Implements ISamplingService interface
   - Provides direct sampling API
   - Supports streaming for large datasets
   - Handles multi-round sampling

3. **PostgresTableReader** (`src/core/infrastructure/postgres/table_reader.py`)
   - Extended with sampling-specific methods
   - `get_column_samples()`: Fast unique value extraction
   - `get_table_sample_stream()`: Streaming sampled data

4. **Database Indexes** (`src/sql/sampling_indexes.sql`)
   - Critical indexes for performance
   - Function-based indexes for JSONB columns
   - Dynamic index creation utilities

## Sampling Methods

### 1. Random Sampling

```python
config = SampleConfig(
    method=SamplingMethod.RANDOM,
    sample_size=1000,
    random_seed=42  # Optional for deterministic results
)
```

**SQL Approaches:**
- **Unseeded**: Uses `TABLESAMPLE SYSTEM` for O(1) performance
- **Seeded (Scalable)**: Hash filtering without sorting, scales to billions
- **Seeded (Exact)**: Uses `ORDER BY` for exact counts on smaller datasets

### 2. Stratified Sampling

```python
config = SampleConfig(
    method=SamplingMethod.STRATIFIED,
    sample_size=5000,
    stratify_columns=['region', 'category'],
    proportional=True,  # Proportional allocation
    random_seed=42
)
```

**Features:**
- Fast strata estimation using TABLESAMPLE
- Single-query execution with hash filtering
- Configurable minimum samples per stratum
- Automatic cardinality checking

### 3. Systematic Sampling

```python
config = SampleConfig(
    method=SamplingMethod.SYSTEMATIC,
    sample_size=100  # Interval calculated automatically
)
```

**Note:** Uses `ROW_NUMBER()` which requires sorting. Use cautiously on large datasets.

### 4. Cluster Sampling

```python
config = SampleConfig(
    method=SamplingMethod.CLUSTER,
    sample_size=1000,
    cluster_column='department_id',
    num_clusters=5,
    random_seed=42
)
```

**Options:**
- Fixed percentage per cluster
- Fixed count per cluster

### 5. Multi-Round Sampling

```python
config = SampleConfig(
    method=SamplingMethod.MULTI_ROUND,
    round_configs=[
        SampleConfig(method=SamplingMethod.RANDOM, sample_size=500),
        SampleConfig(method=SamplingMethod.STRATIFIED, sample_size=500, 
                    stratify_columns=['region'])
    ]
)
```

## Advanced Features

### Dynamic Filtering

```python
'filters': {
    'conditions': [
        {'column': 'age', 'operator': '>', 'value': 21},
        {'column': 'country', 'operator': 'in', 'value': ['US', 'CA']},
        {'column': 'active', 'operator': '=', 'value': True}
    ],
    'logic': 'AND'  # or 'OR'
}
```

**Supported Operators:**
- Comparison: `>`, `>=`, `<`, `<=`, `=`, `!=`
- List: `in`, `not_in`
- Pattern: `like`, `ilike`
- Null: `is_null`, `is_not_null`

### Column Selection and Ordering

```python
'selection': {
    'columns': ['id', 'name', 'age', 'country'],
    'order_by': 'age',
    'order_desc': True
}
```

## Usage Examples

### Direct Sampling API

```python
# Initialize
async with db_pool.acquire() as conn:
    table_reader = PostgresTableReader(conn)
    sampling_service = PostgresSamplingService(db_pool)
    
    # Sample
    result = await sampling_service.sample(
        table_reader, commit_id, 'primary', config
    )
    
    # Access results
    print(f"Sampled {result.sample_size} rows")
    print(f"Strata counts: {result.strata_counts}")
```

### Job-Based Sampling

```python
# Create sampling job
job_service = SamplingJobService(db_pool)
job_id = await job_service.create_sampling_job(
    dataset_id=dataset_id,
    source_commit_id=commit_id,
    user_id=user_id,
    sampling_config={
        'rounds': [{
            'method': 'random',
            'parameters': {'sample_size': 1000, 'seed': 42}
        }]
    }
)

# Check status
status = await job_service.get_job_status(job_id)
```

### Streaming Large Samples

```python
async for row in table_reader.get_table_sample_stream(
    commit_id, 'primary', 'random', {'sample_size': 100000}
):
    # Process row without loading all into memory
    process_row(row)
```

## Performance Optimization

### 1. Create Required Indexes

```sql
-- Run the sampling indexes script
psql -d your_database -f src/sql/sampling_indexes.sql

-- Create column-specific indexes for stratification
SELECT create_sampling_column_index('region');
SELECT create_sampling_column_index('product_category');
```

### 2. Monitor Performance

```sql
-- Check index coverage
SELECT * FROM sampling_index_coverage;

-- Identify columns needing indexes
SELECT * FROM sampling_column_usage;

-- Monitor query performance
SELECT * FROM sampling_query_performance;
```

### 3. Configuration Tuning

```python
# Adjust oversampling factor for better accuracy
executor = SamplingJobExecutor(config={
    'oversampling_factor': 2.0,  # Default: 1.5
    'estimation_sample_percent': 2.0,  # Default: 1.0
    'min_stratum_sample_count': 20  # Default: 10
})
```

## Security Considerations

1. **Column Name Validation**: All column names are validated against SQL injection
2. **Operator Whitelisting**: Only allowed operators are permitted in filters
3. **Parameterized Queries**: All values are passed as parameters, never concatenated
4. **Schema Validation**: Columns are verified against commit schema

## Scalability Guidelines

### Method Selection by Dataset Size

| Dataset Size | Recommended Methods | Avoid |
|-------------|-------------------|--------|
| < 1M rows | All methods | None |
| 1M - 100M | Random (hash), Stratified (hybrid) | Systematic |
| 100M - 1B | Random (TABLESAMPLE), Cluster | Systematic, ORDER BY |
| > 1B | Random (TABLESAMPLE) only | All sorting operations |

### Best Practices

1. **Use TABLESAMPLE for large datasets**: Provides block-level sampling in O(1) time
2. **Prefer hash filtering over ORDER BY**: Eliminates expensive sorts
3. **Create function-based indexes**: Critical for stratified/cluster sampling
4. **Use estimation for large strata**: Avoid full aggregations
5. **Stream results**: Don't load entire samples into memory

## Troubleshooting

### Common Issues

1. **Slow stratified sampling**
   - Solution: Create indexes on stratification columns
   - Check: `SELECT * FROM sampling_column_usage`

2. **Memory errors on large samples**
   - Solution: Use streaming API instead of loading all rows
   - Reduce batch_size in streaming

3. **Inconsistent sample sizes**
   - Solution: Increase oversampling_factor
   - Use exact methods for critical applications

4. **Poor cluster representation**
   - Solution: Increase estimation_sample_percent
   - Verify cluster column has reasonable cardinality

## Future Enhancements

1. **Parallel Sampling**: Process multiple strata/clusters in parallel
2. **Adaptive Sampling**: Automatically adjust parameters based on data distribution
3. **Incremental Sampling**: Add samples to existing sampled datasets
4. **Smart Caching**: Cache estimation results for repeated sampling
5. **LLM-Based Sampling**: Integration with language models for semantic sampling