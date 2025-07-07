# TABLESAMPLE Syntax Error Fix

## The Problem

Job `5953bb6b-dc77-4663-b4cb-ba9eb41dbd67` failed with:
```
syntax error at or near "TABLESAMPLE"
```

## Root Cause

The original query tried to use `TABLESAMPLE` on a JOIN result:

```sql
-- ❌ INVALID - TABLESAMPLE cannot be used after JOIN
WITH source_data AS (
    SELECT m.logical_row_id, m.row_hash, r.data as row_data_json
    FROM dsa_core.commit_rows m
    JOIN dsa_core.rows r ON m.row_hash = r.row_hash
    WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
    TABLESAMPLE SYSTEM($3)  -- ❌ SYNTAX ERROR HERE!
)
```

PostgreSQL's `TABLESAMPLE` clause can only be used directly on a table reference, not on:
- JOIN results
- Subqueries
- CTEs (WITH clauses)

## The Fix

Changed to use `ORDER BY RANDOM()` for unseeded random sampling:

```sql
-- ✅ VALID - Uses ORDER BY RANDOM() instead
WITH source_data AS (
    SELECT m.logical_row_id, m.row_hash, r.data as row_data_json
    FROM dsa_core.commit_rows m
    JOIN dsa_core.rows r ON m.row_hash = r.row_hash
    WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
    ORDER BY RANDOM()  -- ✅ This works!
)
SELECT * FROM source_data LIMIT $3
```

## Performance Considerations

1. **ORDER BY RANDOM()** requires scanning all matching rows, which can be slow for large datasets
2. For better performance with large datasets, use **seeded random sampling** which uses hash-based filtering
3. The seeded approach is much faster as it doesn't require sorting

## When Each Method is Used

- **Unseeded random** (ORDER BY RANDOM()): When no seed parameter is provided
  - Truly random but slower
  - Good for smaller datasets

- **Seeded random** (Hash filtering): When seed parameter is provided
  - Deterministic (same seed = same results)
  - Much faster for large datasets
  - Recommended approach

## Example Usage

### Without seed (uses fixed query):
```json
{
  "method": "random",
  "parameters": {
    "sample_size": 1000
  }
}
```

### With seed (uses optimized query):
```json
{
  "method": "random",
  "parameters": {
    "sample_size": 1000,
    "seed": 42
  }
}
```

## Testing the Fix

After applying the fix, new sampling jobs should work correctly. To verify:

1. Create a new sampling job
2. Check that it completes successfully
3. For better performance, always provide a seed value

## Alternative Approaches (Not Implemented)

If ORDER BY RANDOM() is too slow, other options include:

1. **Reservoir sampling** in application code
2. **Pre-computed random columns** in the table
3. **Approximate sampling** using statistics