# SQL-Based Sampling Implementation Evaluation

## Executive Summary

After analyzing the DSA platform's schema and interfaces, I've identified several opportunities to implement SQL-based sampling that aligns with existing patterns while adhering to DRY principles. The current architecture is well-suited for SQL-based sampling, with a clear separation of concerns through the ITableReader interface and PostgreSQL-based storage.

## Current Architecture Analysis

### 1. Database Schema Structure

The platform uses a Git-like versioning system with the following key tables:

```sql
-- Content-addressable storage
dsa_core.rows (row_hash, data JSONB)
dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
dsa_core.commits (commit_id, dataset_id, parent_commit_id, ...)
```

**Key Insights:**
- Data is stored in JSONB format in the `rows` table
- The `commit_rows` table acts as a manifest linking commits to their rows
- Logical row IDs follow patterns like `table_key:row_idx` or `table_key_row_idx`

### 2. Interface Architecture

The platform follows a clean architecture with well-defined abstractions:

```
IUnitOfWork
├── ICommitRepository
├── IDatasetRepository  
├── ITableReader (key interface for data access)
└── IJobRepository

Service Interfaces:
├── ISamplingService (defined but not implemented)
├── IStatisticsService
├── IExplorationService
└── IWorkbenchService
```

### 3. Existing Data Access Patterns

The `PostgresTableReader` already implements efficient SQL-based data access:
- Paginated queries with OFFSET/LIMIT
- Streaming support for large datasets
- Table-aware filtering using logical_row_id patterns

## SQL-Based Sampling Implementation Strategy

### 1. Leverage Existing ITableReader Interface

The `ITableReader` interface is the natural place to add sampling capabilities:

```python
class ITableReader(ABC):
    # Existing methods...
    
    @abstractmethod
    async def get_table_sample(
        self,
        commit_id: str,
        table_key: str,
        sample_size: Union[int, float],
        method: str = "random",
        seed: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get a sample of rows from a table."""
        pass
```

### 2. PostgreSQL Sampling Techniques

#### a. Random Sampling (Most Efficient)
```sql
-- Using TABLESAMPLE for approximate sampling (PostgreSQL 9.5+)
SELECT r.data, cr.logical_row_id
FROM dsa_core.commit_rows cr
TABLESAMPLE SYSTEM (10) -- 10% sample
JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE $2

-- For exact row count with better randomness
SELECT r.data, cr.logical_row_id
FROM dsa_core.commit_rows cr
JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE $2
ORDER BY RANDOM()
LIMIT $3
```

#### b. Stratified Sampling
```sql
-- Using window functions for stratified sampling
WITH stratified AS (
    SELECT 
        r.data,
        cr.logical_row_id,
        r.data->>'category' as stratum,
        ROW_NUMBER() OVER (PARTITION BY r.data->>'category' ORDER BY RANDOM()) as rn
    FROM dsa_core.commit_rows cr
    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
    WHERE cr.commit_id = $1 AND cr.logical_row_id LIKE $2
)
SELECT data, logical_row_id
FROM stratified
WHERE rn <= $3 -- samples per stratum
```

#### c. Systematic Sampling
```sql
-- Every Nth row
SELECT r.data, cr.logical_row_id
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY logical_row_id) as rn
    FROM dsa_core.commit_rows
    WHERE commit_id = $1 AND logical_row_id LIKE $2
) cr
JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
WHERE cr.rn % $3 = 0 -- sampling interval
```

### 3. Implementation in PostgresTableReader

Extend the existing `PostgresTableReader` class:

```python
async def get_table_sample(
    self,
    commit_id: str,
    table_key: str,
    sample_size: Union[int, float],
    method: str = "random",
    seed: Optional[int] = None,
    stratify_column: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get a sample of rows from a table using SQL-based sampling.
    
    Reuses existing pattern matching and data parsing logic.
    """
    pattern = f"{table_key}:%" if ':' in table_key else f"{table_key}_%"
    
    if seed is not None:
        # Set seed for reproducible sampling
        await self._conn.execute(f"SELECT setseed({seed/1000.0})")
    
    if method == "random":
        if isinstance(sample_size, float) and sample_size < 1.0:
            # Percentage-based sampling
            query = """
                SELECT r.data, cr.logical_row_id
                FROM dsa_core.commit_rows cr
                TABLESAMPLE SYSTEM ($3)
                JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = $1 
                AND (cr.logical_row_id LIKE $2 OR r.data->>'sheet_name' = $4)
                ORDER BY RANDOM()
            """
            rows = await self._conn.fetch(
                query, commit_id, pattern, sample_size * 100, table_key
            )
        else:
            # Fixed row count sampling
            query = """
                SELECT r.data, cr.logical_row_id
                FROM dsa_core.commit_rows cr
                JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                WHERE cr.commit_id = $1 
                AND (cr.logical_row_id LIKE $2 OR r.data->>'sheet_name' = $3)
                ORDER BY RANDOM()
                LIMIT $4
            """
            rows = await self._conn.fetch(
                query, commit_id, pattern, table_key, int(sample_size)
            )
    
    # Reuse existing data parsing logic
    return self._parse_rows(rows)
```

### 4. Implement ISamplingService

Create a concrete implementation that leverages the enhanced ITableReader:

```python
class PostgresSamplingService(ISamplingService):
    """SQL-based sampling service implementation."""
    
    def __init__(self):
        self._strategies = {
            SamplingMethod.RANDOM: self._random_sample,
            SamplingMethod.STRATIFIED: self._stratified_sample,
            SamplingMethod.SYSTEMATIC: self._systematic_sample,
        }
    
    async def sample(
        self,
        table_reader: ITableReader,
        commit_id: str,
        table_key: str,
        config: SampleConfig
    ) -> SampleResult:
        """
        Perform sampling using SQL-based methods.
        Delegates to table_reader for actual data access.
        """
        strategy = self._strategies.get(config.method)
        if not strategy:
            raise ValueError(f"Unsupported sampling method: {config.method}")
        
        # Use the table reader's SQL-based sampling
        sampled_data = await table_reader.get_table_sample(
            commit_id=commit_id,
            table_key=table_key,
            sample_size=config.sample_size,
            method=config.method.value,
            seed=config.random_seed,
            stratify_column=config.stratify_columns[0] if config.stratify_columns else None
        )
        
        return SampleResult(
            sampled_data=sampled_data,
            sample_size=len(sampled_data),
            method_used=config.method,
            metadata={
                "commit_id": commit_id,
                "table_key": table_key,
                "seed": config.random_seed
            }
        )
```

## DRY Principle Adherence

### 1. Reuse Existing Components
- **ITableReader**: Extend rather than duplicate data access logic
- **PostgresTableReader**: Reuse connection management, pattern matching, and data parsing
- **UnitOfWork**: Leverage existing transaction management

### 2. Avoid Duplication
- Use the same logical_row_id pattern matching as existing methods
- Reuse data parsing logic from `get_table_data()`
- Share connection and transaction handling through UoW

### 3. Consistent Patterns
- Follow the same error handling patterns
- Use the same pagination approach (offset/limit)
- Maintain consistency with existing query patterns

## Performance Considerations

### 1. PostgreSQL TABLESAMPLE
- **Pros**: Very fast for large tables, minimal I/O
- **Cons**: Approximate sampling, requires PostgreSQL 9.5+
- **Use Case**: Quick exploratory sampling

### 2. ORDER BY RANDOM()
- **Pros**: True random sampling, works on any PostgreSQL version
- **Cons**: Requires full table scan, slower on large tables
- **Use Case**: Precise sampling when accuracy matters

### 3. Window Functions
- **Pros**: Efficient for stratified/systematic sampling
- **Cons**: More complex queries, memory usage for large result sets
- **Use Case**: Advanced sampling methods

## Recommendations

### 1. Start with Simple Random Sampling
Implement basic random sampling in `PostgresTableReader` first:
- Use `TABLESAMPLE` for speed when approximate is OK
- Fall back to `ORDER BY RANDOM()` for exact counts
- Add seed support for reproducibility

### 2. Extend Incrementally
Add advanced sampling methods as needed:
- Stratified sampling using window functions
- Systematic sampling with modulo operations
- Cluster sampling with GROUP BY

### 3. Cache Sampling Results
For frequently sampled datasets:
- Store sample references in `commit_statistics`
- Reuse samples when parameters match
- Invalidate on new commits

### 4. Monitor Performance
Add metrics for:
- Sampling query execution time
- Memory usage for large samples
- Cache hit rates

## Implementation Priority

1. **Phase 1**: Basic random sampling in PostgresTableReader
2. **Phase 2**: PostgresSamplingService with random method
3. **Phase 3**: Stratified and systematic sampling
4. **Phase 4**: Performance optimizations and caching

## Conclusion

The DSA platform's architecture is well-suited for SQL-based sampling. By extending the existing `ITableReader` interface and leveraging PostgreSQL's sampling capabilities, we can implement efficient sampling while maintaining DRY principles and consistency with existing patterns. The proposed approach minimizes code duplication and maximizes reuse of existing infrastructure.