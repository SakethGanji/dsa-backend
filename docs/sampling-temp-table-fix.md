# Temporary Table Collision Fix

## The Problem

Job `46b66be2-25d0-47f1-8f57-933546f7dbf4` failed with:
```
relation "temp_round_1_samples" already exists
```

## Root Cause

PostgreSQL temporary tables are session-specific, but when using connection pooling:
- Connections are reused between jobs
- Temporary tables from previous jobs might still exist
- Creating a table with the same name causes a collision

## The Fix

Added proper cleanup for temporary tables:

1. **Before creating**: Always drop the table if it exists
   ```sql
   DROP TABLE IF EXISTS temp_round_1_samples
   ```

2. **After job completion**: Clean up all temporary tables
   ```sql
   DROP TABLE IF EXISTS temp_round_1_samples;
   DROP TABLE IF EXISTS temp_round_2_samples;
   -- etc.
   ```

## Code Changes

### 1. Drop Before Create
```python
# Always drop table if it exists to ensure clean state
await conn.execute(f"DROP TABLE IF EXISTS {round_table}")
```

### 2. Cleanup After Job
```python
# Clean up temporary tables
for round_idx in range(len(round_results)):
    round_table = f"temp_round_{round_idx + 1}_samples"
    await conn.execute(f"DROP TABLE IF EXISTS {round_table}")

# Also drop residual table if it was created
await conn.execute("DROP TABLE IF EXISTS temp_residual_data")
```

## Why This Happened

1. **Connection Pooling**: The database connection pool reuses connections
2. **No Automatic Cleanup**: Temporary tables persist for the session lifetime
3. **Fixed Names**: Table names like `temp_round_1_samples` can collide

## Alternative Solutions (Not Implemented)

1. **Unique Table Names**: Include job ID in table name
   ```python
   round_table = f"temp_round_{round_number}_samples_{job_id[:8]}"
   ```

2. **Use CTEs Instead**: Common Table Expressions don't persist
   ```sql
   WITH temp_round_1 AS (...)
   ```

3. **Transaction-Scoped Tables**: Use `ON COMMIT DROP`
   ```sql
   CREATE TEMP TABLE ... ON COMMIT DROP
   ```

## Testing

After this fix:
1. Jobs should not fail with "relation already exists" errors
2. Multiple jobs can run sequentially without conflicts
3. Connection pool reuse won't cause issues

## Best Practices

1. **Always clean up** temporary resources
2. **Use unique names** when possible
3. **Handle cleanup in finally blocks** for error cases
4. **Consider CTEs** for temporary data that doesn't need indexing