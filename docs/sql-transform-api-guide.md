# SQL Transform API Complete Guide

## Overview

The `/api/workbench/sql-transform` endpoint provides a unified interface for SQL operations on your datasets. It supports two primary modes:

1. **Preview Mode** (`save=false`) - Test queries and see results immediately
2. **Save Mode** (`save=true`) - Execute transformations and create new dataset commits

## Endpoint Details

- **URL**: `POST /api/workbench/sql-transform`
- **Authentication**: Bearer token required
- **Content-Type**: `application/json`

## Request Schema

```typescript
{
  // Required: Data sources for your query
  sources: Array<{
    alias: string;       // Table alias used in SQL (e.g., "customers", "c")
    dataset_id: number;  // Dataset ID to query
    ref: string;        // Branch/tag name (e.g., "main", "v1.0")
    table_key: string;  // Table within dataset (usually "default")
  }>;
  
  // Required: SQL query to execute
  sql: string;
  
  // Mode selection (defaults to false)
  save: boolean;
  
  // Required when save=true
  target?: {
    dataset_id: number;
    ref: string;
    table_key: string;
    message: string;
    output_branch_name?: string;
    expected_head_commit_id?: string;
    create_new_dataset?: boolean;       // Create a new dataset instead of updating
    new_dataset_name?: string;          // Name for new dataset (required if create_new_dataset=true)
    new_dataset_description?: string;   // Description for new dataset
  };
  
  // Preview mode options
  limit?: number;         // Max rows (1-10000, default: 1000)
  offset?: number;        // Pagination offset (default: 0)
  quick_preview?: boolean;    // Enable fast sampling (default: false)
  sample_percent?: number;    // Sample size when quick_preview=true (0.1-100, default: 1.0)
  
  // Validation only
  dry_run?: boolean;      // Validate without executing (default: false)
}
```

## Response Schema

### Preview Mode Response
```typescript
{
  data: Array<Record<string, any>>;  // Query results
  row_count: number;                  // Rows returned
  total_row_count?: number;           // Total available (if known)
  execution_time_ms: number;          // Query execution time
  columns: Array<{                    // Column metadata
    name: string;
    type: string;
  }>;
  has_more: boolean;                  // More rows available?
}
```

### Save Mode Response
```typescript
{
  job_id: string;         // Job ID for tracking
  status: string;         // "pending", "running", "completed", "failed"
  estimated_rows?: number; // Estimated rows to process
}
```

## Usage Examples

### 1. Basic Preview Query

Test a simple SELECT query:

```bash
curl -X POST http://localhost:8000/api/workbench/sql-transform \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [{
      "alias": "customers",
      "dataset_id": 123,
      "ref": "main",
      "table_key": "default"
    }],
    "sql": "SELECT * FROM customers WHERE age > 25",
    "save": false,
    "limit": 10
  }'
```

**Response:**
```json
{
  "data": [
    {"id": 1, "name": "John Doe", "age": 30, "city": "New York"},
    {"id": 2, "name": "Jane Smith", "age": 28, "city": "Boston"}
  ],
  "row_count": 2,
  "execution_time_ms": 45,
  "columns": [
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "VARCHAR"},
    {"name": "age", "type": "INTEGER"},
    {"name": "city", "type": "VARCHAR"}
  ],
  "has_more": true
}
```

### 2. Quick Preview with Sampling (NEW!)

For large datasets, use quick preview for fast approximate results:

```json
{
  "sources": [{
    "alias": "events",
    "dataset_id": 456,
    "ref": "main",
    "table_key": "default"
  }],
  "sql": "SELECT event_type, COUNT(*) as cnt FROM events GROUP BY event_type",
  "save": false,
  "quick_preview": true,
  "sample_percent": 1.0,  // Sample 1% of data
  "limit": 100
}
```

**Response:**
```json
{
  "data": [
    {"event_type": "click", "cnt": 523},
    {"event_type": "view", "cnt": 1847},
    {"event_type": "purchase", "cnt": 89}
  ],
  "row_count": 3,
  "execution_time_ms": 12,
  "columns": [
    {"name": "event_type", "type": "VARCHAR"},
    {"name": "cnt", "type": "INTEGER"}
  ],
  "has_more": false
}
```

> **Note**: Quick preview results are APPROXIMATE. Use for testing query logic, not for exact counts.

### 3. Multi-Source Join Query

Join data from multiple datasets:

```json
{
  "sources": [
    {
      "alias": "orders",
      "dataset_id": 100,
      "ref": "main",
      "table_key": "default"
    },
    {
      "alias": "customers",
      "dataset_id": 101,
      "ref": "main",
      "table_key": "default"
    }
  ],
  "sql": "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
  "save": false,
  "limit": 20
}
```

### 4. Save Transformation (Create New Commit)

Execute a transformation and save results:

```json
{
  "sources": [{
    "alias": "raw_data",
    "dataset_id": 200,
    "ref": "main",
    "table_key": "default"
  }],
  "sql": "SELECT *, UPPER(name) as name_upper, age * 12 as age_months FROM raw_data",
  "save": true,
  "target": {
    "dataset_id": 200,
    "ref": "main",
    "table_key": "enriched",
    "message": "Added uppercase names and age in months",
    "output_branch_name": "feature/data-enrichment"
  }
}
```

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "estimated_rows": 50000
}
```

Then check job status:
```bash
GET /api/jobs/550e8400-e29b-41d4-a716-446655440000
```

### 5. Pagination for Large Results

Navigate through large result sets:

```json
{
  "sources": [{
    "alias": "logs",
    "dataset_id": 300,
    "ref": "main",
    "table_key": "default"
  }],
  "sql": "SELECT * FROM logs ORDER BY timestamp DESC",
  "save": false,
  "limit": 50,
  "offset": 100  // Skip first 100 rows
}
```

### 6. Creating a New Dataset

Create a new dataset from transformation results:

```json
{
  "sources": [{
    "alias": "raw",
    "dataset_id": 100,
    "ref": "main",
    "table_key": "default"
  }],
  "sql": "SELECT customer_id, SUM(amount) as total_spent FROM raw GROUP BY customer_id",
  "save": true,
  "target": {
    "dataset_id": 999,  // Can be any number - will be ignored
    "ref": "main",
    "table_key": "default",
    "message": "Created customer spending summary dataset",
    "create_new_dataset": true,
    "new_dataset_name": "Customer Spending Summary",
    "new_dataset_description": "Aggregated customer spending data derived from raw transactions"
  }
}
```

### 7. Optimistic Locking for Concurrent Updates

Prevent race conditions when multiple users update the same dataset:

```json
{
  "sources": [{
    "alias": "src",
    "dataset_id": 400,
    "ref": "main",
    "table_key": "default"
  }],
  "sql": "SELECT * FROM src WHERE status = 'pending'",
  "save": true,
  "target": {
    "dataset_id": 400,
    "ref": "main",
    "table_key": "processed",
    "message": "Processed pending records",
    "expected_head_commit_id": "abc123def456..."  // Current commit ID
  }
}
```

If another update happens first, you'll get a 500 error with a message about the concurrent update.

## Quick Preview Deep Dive

### How It Works

Quick preview uses the `execute_sql_with_sampled_sources` method which applies sampling at the source level:

```sql
-- The implementation creates CTEs with sampling for each source table
-- For example, with sample_percent=1.0:

WITH source_customers AS (
  -- Random sampling happens at the commit_rows level
  SELECT cr.logical_row_id, r.data
  FROM dsa_core.commit_rows cr
  JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
  WHERE cr.commit_id = 'abc123...'
    AND random() < 0.01  -- 1% sampling
),
-- User's SQL is then executed on the sampled data
user_query AS (
  SELECT * FROM source_customers WHERE age > 25
)
SELECT * FROM user_query LIMIT 1000
```

Note: The actual implementation uses `execute_sql_with_sampled_sources` which dynamically constructs the sampled CTEs based on the sources in your query.

### When to Use Quick Preview

✅ **Good for:**
- Testing query logic during development
- Getting approximate aggregations
- Validating JOIN conditions
- Checking data transformations

❌ **Not suitable for:**
- Exact counts or statistics
- Financial calculations
- Data validation requiring all rows
- Production transformations

### Sample Percentage Guidelines

| Data Size | Recommended % | Use Case |
|-----------|---------------|----------|
| < 1K rows | 100% | Full scan is fast |
| 1K-10K | 10-50% | Good balance |
| 10K-100K | 1-10% | Reasonable sample |
| 100K-1M | 0.1-1% | Large datasets |
| > 1M | 0.1% | Very large datasets |

## Error Handling

### Common Error Responses

#### 400 Bad Request
```json
{
  "error": "VALIDATION_ERROR",
  "message": "SQL execution failed: column not found",
  "request_id": "uuid"
}
```

#### 403 Forbidden
```json
{
  "error": "FORBIDDEN",
  "message": "Access denied to dataset 123",
  "request_id": "uuid"
}
```

#### 422 Unprocessable Entity (Pydantic Validation Error)
```json
{
  "detail": [
    {
      "loc": ["body", "target"],
      "msg": "target is required when save is True",
      "type": "value_error"
    }
  ]
}
```

Note: This is the standard FastAPI/Pydantic validation error format. The custom error handler may transform this into a different format with `error: "VALIDATION_ERROR"` structure.

#### 500 Internal Server Error (Optimistic Locking Conflict)
```json
{
  "error": "INTERNAL_SERVER_ERROR",
  "message": "Concurrent update detected: ref 'main' was updated by another transaction. Expected commit abc123def456... but ref has moved. Please retry your transformation with the latest commit.",
  "request_id": "uuid"
}
```

Note: In the current implementation, optimistic locking conflicts throw a generic exception that results in a 500 error. In a production environment, this would ideally be caught and returned as a 409 Conflict.

## Best Practices

### 1. Development Workflow
```python
# Step 1: Test with quick preview
{
  "sql": "SELECT complex_transformation FROM data",
  "quick_preview": true,
  "sample_percent": 1.0,
  "limit": 10
}

# Step 2: Validate with full preview
{
  "sql": "SELECT complex_transformation FROM data",
  "quick_preview": false,
  "limit": 100
}

# Step 3: Save transformation
{
  "sql": "SELECT complex_transformation FROM data",
  "save": true,
  "target": {...}
}
```

### 2. Performance Tips

- Start with small `limit` values during development
- Use `quick_preview` for iterative testing
- Add WHERE clauses to filter data early
- Index columns used in JOINs and WHERE clauses

### 3. Safety Guidelines

- Always test queries in preview mode first
- Use descriptive commit messages
- Consider using branches for experimental transformations
- Implement optimistic locking for critical updates

## Advanced Features

### Dry Run Validation
```json
{
  "sources": [...],
  "sql": "SELECT * FROM data",
  "save": true,
  "dry_run": true,  // Validate only, don't execute
  "target": {...}
}
```

### Creating New Branches
```json
{
  "target": {
    "dataset_id": 123,
    "ref": "main",
    "table_key": "transformed",
    "message": "Experimental transformation",
    "output_branch_name": "experiment/new-feature"  // Creates new branch
  }
}
```

### Multiple Table Keys
```json
{
  "sources": [
    {
      "alias": "sales",
      "dataset_id": 100,
      "ref": "main",
      "table_key": "default"  // Main data
    },
    {
      "alias": "metadata",
      "dataset_id": 100,
      "ref": "main", 
      "table_key": "metadata"  // Metadata table
    }
  ],
  "sql": "SELECT s.*, m.category FROM sales s JOIN metadata m ON s.id = m.id"
}
```

## Troubleshooting

### Query Times Out
- Use `quick_preview` with small sample percentage
- Add WHERE clauses to reduce data
- Check for missing indexes

### No Results with Quick Preview
- Increase `sample_percent` - small datasets need higher percentages
- Remember sampling is random - results vary between runs

### Memory Errors
- Always use `save=true` for large transformations
- Server-side processing handles any size PostgreSQL supports

### Concurrent Update Conflicts
- Fetch latest commit ID
- Include `expected_head_commit_id` in target
- Retry with updated commit ID if conflict occurs

## Summary

The SQL Transform API provides a powerful, Git-like interface for data transformations:

- **Preview mode** for testing and exploration
- **Quick preview** for fast approximate results  
- **Save mode** for creating immutable commits
- **Multi-source joins** for complex queries
- **Optimistic locking** for safe concurrent updates

Use preview mode during development, leverage quick preview for large datasets, and save transformations to create an auditable history of your data pipeline.