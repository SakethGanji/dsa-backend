# SQL Transform Endpoint Guide

## Overview

The `/api/workbench/sql-transform` endpoint provides a unified interface for both previewing SQL query results and executing SQL transformations that create new dataset commits. This follows the Git-like versioning model where every transformation creates an immutable snapshot.

## Endpoint Details

**URL**: `POST /api/workbench/sql-transform`  
**Authentication**: Required (Bearer token)

## Request/Response Modes

### 1. Preview Mode (save=false)

Preview mode allows you to test SQL queries on your data without making any changes. Results are paginated to prevent large data transfers.

#### Request Example
```json
{
  "sources": [
    {
      "alias": "customers",
      "dataset_id": 123,
      "ref": "main",
      "table_key": "default"
    }
  ],
  "sql": "SELECT * FROM customers WHERE age > 25",
  "save": false,
  "limit": 100,
  "offset": 0
}
```

#### Response Example
```json
{
  "data": [
    {"id": 1, "name": "John Doe", "age": 30, "city": "New York"},
    {"id": 2, "name": "Jane Smith", "age": 28, "city": "Boston"}
  ],
  "row_count": 2,
  "total_row_count": null,
  "execution_time_ms": 45,
  "columns": [
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "VARCHAR"},
    {"name": "age", "type": "INTEGER"},
    {"name": "city", "type": "VARCHAR"}
  ],
  "has_more": false
}
```

### 2. Save Mode (save=true)

Save mode executes the transformation asynchronously and creates a new commit in the dataset's history.

#### Request Example
```json
{
  "sources": [
    {
      "alias": "sales",
      "dataset_id": 456,
      "ref": "main",
      "table_key": "default"
    },
    {
      "alias": "products",
      "dataset_id": 789,
      "ref": "v1.0",
      "table_key": "default"
    }
  ],
  "sql": "SELECT s.*, p.name as product_name, p.category FROM sales s JOIN products p ON s.product_id = p.id",
  "save": true,
  "target": {
    "dataset_id": 456,
    "ref": "main",
    "table_key": "enriched_sales",
    "message": "Added product details to sales data",
    "output_branch_name": "feature/enriched-sales",
    "expected_head_commit_id": "abc123..."  // Optional: prevents concurrent update conflicts
  }
}
```

#### Response Example
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "estimated_rows": 10000
}
```

## How It Works with the Schema

### Preview Mode Flow

1. **Source Resolution**: For each source, the system:
   - Looks up the dataset in `dsa_core.datasets`
   - Resolves the ref (e.g., "main") to a commit_id via `dsa_core.refs`
   - Identifies the specific commit snapshot to query

2. **Data Access**: The query executes against:
   - Data from `dsa_core.rows` (content-addressable storage)
   - Using the manifest in `dsa_core.commit_rows` for the resolved commit
   - Each commit provides an immutable view of the data at that point

3. **Query Execution**: 
   - SQL is executed with pagination (LIMIT/OFFSET)
   - Multiple sources can be joined using their aliases
   - Results are returned directly without any persistence

### Save Mode Flow

1. **Job Creation**: An async job is created in `dsa_jobs.analysis_runs`

2. **Transformation Execution**: The job worker:
   - Resolves all source datasets and refs to specific commits
   - Executes the SQL transformation
   - Collects all result rows

3. **Commit Creation**:
   ```sql
   -- New commit is created
   INSERT INTO dsa_core.commits (commit_id, dataset_id, parent_commit_id, message, author_id)
   VALUES (
     'sha256_hash',           -- Generated commit hash
     456,                     -- Target dataset_id
     'previous_commit_hash',  -- Parent from current ref
     'Added product details...', -- Commit message
     user_id                  -- Current user
   );
   ```

4. **Data Storage**:
   ```sql
   -- Each unique row is stored (content-addressable)
   INSERT INTO dsa_core.rows (row_hash, data)
   VALUES ('row_sha256', '{"id": 1, "product_name": "Widget", ...}')
   ON CONFLICT DO NOTHING;

   -- Link rows to the new commit
   INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
   VALUES ('sha256_hash', 'row_1', 'row_sha256');
   ```

5. **Reference Updates**:
   ```sql
   -- Update the target ref to point to new commit
   UPDATE dsa_core.refs 
   SET commit_id = 'new_commit_hash'
   WHERE dataset_id = 456 AND name = 'main';

   -- Optionally create a new branch
   INSERT INTO dsa_core.refs (dataset_id, name, commit_id)
   VALUES (456, 'feature/enriched-sales', 'new_commit_hash');
   ```

## Key Features

### Multi-Source Joins
You can join data from multiple datasets at different versions:
```sql
SELECT 
  c.*, 
  o.order_date,
  p.product_name
FROM customers c
JOIN orders o ON c.id = o.customer_id  
JOIN products p ON o.product_id = p.id
```

### Table Keys
Each dataset can have multiple logical tables via `table_key`:
- `default`: The main table
- `metadata`: Additional metadata table
- `enriched_sales`: Transformed data table

### Version Control Benefits
- **Immutability**: Previous commits are never modified
- **Lineage**: Full transformation history via parent_commit_id
- **Branching**: Create feature branches for experiments
- **Time Travel**: Query any historical version via refs

## Request Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| sources | Array[SqlSource] | Yes | Source tables to query |
| sql | string | Yes | SQL query to execute |
| save | boolean | No | Whether to save results (default: false) |
| target | SqlTransformTarget | When save=true | Where to save results |
| limit | integer | No | Max rows for preview (default: 1000) |
| offset | integer | No | Pagination offset (default: 0) |
| dry_run | boolean | No | Validate without executing |

### SqlSource Structure
```typescript
{
  alias: string;        // Table alias for SQL query
  dataset_id: number;   // Dataset to query
  ref: string;         // Branch/tag name (e.g., "main", "v1.0")
  table_key: string;   // Logical table within dataset
}
```

### SqlTransformTarget Structure
```typescript
{
  dataset_id: number;           // Dataset to update
  ref: string;                 // Ref to update (e.g., "main")
  table_key: string;           // Table to create/update
  message: string;             // Commit message
  output_branch_name?: string; // Optional new branch name
  expected_head_commit_id?: string; // Optional: current commit for optimistic locking
  create_new_dataset?: boolean;// Create new dataset (future)
}
```

## Error Handling

Common errors:
- `404`: Dataset or ref not found
- `403`: Insufficient permissions
- `422`: Validation error (e.g., missing target when save=true)
- `400`: SQL syntax error or invalid query
- `409`: Concurrent update conflict (when using expected_head_commit_id)

## Best Practices

1. **Test with Preview First**: Always test complex queries with `save=false`
2. **Use Descriptive Messages**: Help track transformation history
3. **Branch for Experiments**: Use `output_branch_name` for experimental transforms
4. **Monitor Jobs**: Check job status via `/api/jobs/{job_id}` after save mode
5. **Pagination**: Use limit/offset for large preview results
6. **Prevent Race Conditions**: Use `expected_head_commit_id` for concurrent workflows
7. **Handle Conflicts**: Retry with latest commit if optimistic locking fails

## Concurrency and Safety Features

### Optimistic Locking
To prevent lost updates in concurrent workflows, include the current commit ID:

```json
{
  "target": {
    "dataset_id": 456,
    "ref": "main",
    "expected_head_commit_id": "current_commit_hash",
    // ... other fields
  }
}
```

If another transformation completes first, your request will fail with a clear error message, allowing you to:
1. Fetch the latest commit
2. Review any conflicts
3. Retry your transformation

### Transactional Guarantees
All commit operations are wrapped in a database transaction:
- Commit creation
- Row storage  
- Ref updates
- Branch creation

Either all succeed or all roll back - no partial commits.

### Default Safety
The `save` parameter now defaults to `false`, preventing accidental transformations. You must explicitly set `save: true` to create commits.

### Server-Side Processing
When `save=true`, the transformation is executed entirely within PostgreSQL:
- **Zero memory footprint**: No data is loaded into the application
- **Handles billions of rows**: PostgreSQL processes the transformation efficiently
- **Single SQL statement**: All inserts happen in one atomic operation
- **Content deduplication**: Still maintains efficient storage via row hashing

The system uses a sophisticated CTE (Common Table Expression) pipeline:
```sql
WITH transformation_results AS (
    -- Your SQL transformation
    SELECT * FROM sales JOIN products ON ...
),
prepared_rows AS (
    -- Prepare data with hashing
    SELECT data, sha256_hash, row_number...
),
row_inserts AS (
    -- Insert unique rows
    INSERT INTO dsa_core.rows ...
)
-- Link rows to commit
INSERT INTO dsa_core.commit_rows ...
```

This approach scales to any dataset size that PostgreSQL can handle.

## Performance Optimizations for IDE Usage

Since this API is designed for IDE-like environments where users iteratively test queries, several optimizations are available:

### Preview Mode Optimizations

1. **Smart LIMIT Injection**: For simple SELECT queries without aggregations, the LIMIT is injected directly into the query rather than wrapping it. This allows PostgreSQL to stop scanning early.

2. **Query Result Caching**: Preview results are cached for 5 minutes, so re-running the same query (common during development) returns instantly.

3. **Quick Preview Mode**: Set `quick_preview: true` to use table sampling for approximate but fast results on large datasets.

### Best Practices for IDE Usage

When developing queries iteratively:

1. **Start with small limits**: Use `limit: 10` while developing your query logic
2. **Add WHERE clauses early**: Filter data before expensive operations
3. **Use quick_preview for large datasets**: Get approximate results instantly
4. **Save incrementally**: Create branches for experimental transformations

Example for iterative development:
```json
{
  "sql": "SELECT * FROM sales WHERE region = 'WEST'",
  "save": false,
  "limit": 10,  // Small limit for quick iteration
  "quick_preview": true  // Fast approximate results
}
```

The caching and query optimization ensure that repeated executions during development are fast and don't unnecessarily load the database.