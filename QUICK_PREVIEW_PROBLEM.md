# Quick Preview Feature - Problem Statement

## Background
The SQL workbench has a `quick_preview` parameter intended to provide fast approximate results for large datasets. This is useful in IDE-like environments where users iteratively test queries before running the full transformation.

## Current Implementation Problem

### What We Tried
When `quick_preview: true`, we attempt to use PostgreSQL's `TABLESAMPLE` clause:

```sql
-- Intended optimization:
SELECT * FROM source_data TABLESAMPLE SYSTEM (1)  -- 1% sample
```

### Why It Doesn't Work
Our data access pattern uses CTEs (Common Table Expressions):

```sql
-- How we actually access data:
WITH source_data AS (
    SELECT cr.logical_row_id, r.data
    FROM dsa_core.commit_rows cr
    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
    WHERE cr.commit_id = 'abc123...'
)
SELECT * FROM source_data  -- This is a CTE, not a table!
```

**PostgreSQL Error**: "TABLESAMPLE clause can only be applied to tables and materialized views"

### Current Behavior
- `quick_preview: true` tries TABLESAMPLE, fails, falls back to regular query
- Users get exact same results whether quick_preview is true or false
- No actual performance benefit

## Potential Solutions

### Option 1: Random Sampling with WHERE
```sql
WITH source_data AS (
    SELECT cr.logical_row_id, r.data
    FROM dsa_core.commit_rows cr
    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
    WHERE cr.commit_id = 'abc123...'
    AND random() < 0.01  -- 1% sample
)
SELECT * FROM source_data
```
**Pros**: Simple, works with CTEs
**Cons**: Still reads all rows (just filters most out)

### Option 2: TABLESAMPLE on Base Tables
```sql
WITH source_data AS (
    SELECT cr.logical_row_id, r.data
    FROM dsa_core.commit_rows cr TABLESAMPLE SYSTEM (1)
    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
    WHERE cr.commit_id = 'abc123...'
)
SELECT * FROM source_data
```
**Pros**: True sampling at table level
**Cons**: Might miss rows due to join, biased sample

### Option 3: Materialized View
Create a materialized view for each commit, then TABLESAMPLE works:
```sql
CREATE MATERIALIZED VIEW mv_commit_abc123 AS
SELECT cr.logical_row_id, r.data
FROM dsa_core.commit_rows cr
JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
WHERE cr.commit_id = 'abc123...';

-- Then:
SELECT * FROM mv_commit_abc123 TABLESAMPLE SYSTEM (1);
```
**Pros**: TABLESAMPLE works perfectly
**Cons**: Storage overhead, maintenance complexity

### Option 4: Skip-scan with MOD
```sql
WITH numbered_data AS (
    SELECT cr.logical_row_id, r.data,
           ROW_NUMBER() OVER () as rn
    FROM dsa_core.commit_rows cr
    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
    WHERE cr.commit_id = 'abc123...'
)
SELECT * FROM numbered_data
WHERE MOD(rn, 100) = 0  -- Every 100th row
```
**Pros**: Deterministic, even sampling
**Cons**: Still needs to number all rows first

## My Recommendation

**Option 1 (Random sampling with WHERE)** seems best because:
1. Simple to implement
2. Works with existing CTE structure
3. Provides true random sampling
4. While it reads all rows, it avoids the expensive join for 99% of them

The key insight is to apply the filter BEFORE the join:
```sql
WITH source_data AS (
    SELECT cr.logical_row_id, r.data
    FROM dsa_core.commit_rows cr
    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
    WHERE cr.commit_id = 'abc123...'
    AND random() < 0.01  -- Filter happens before expensive join
)
```

## Questions for the Team

1. Is approximate sampling acceptable for preview mode? (Some rows will be randomly excluded)
2. What's the typical size of commit_rows for a single commit? (Helps choose sampling %)
3. Should sampling be deterministic (same results each time) or random?
4. Is there a better PostgreSQL-specific technique we're missing?

## Current Workaround
The code currently catches TABLESAMPLE errors and falls back to regular queries, so users still get results - just not the performance benefit of sampling.