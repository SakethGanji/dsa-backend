# Sampling Features Guide

This guide covers all sampling capabilities in the Data Sampling & Analysis (DSA) platform, including multi-round sampling, residual datasets, and advanced filtering options.

## Table of Contents

1. [Overview](#overview)
2. [API Endpoints](#api-endpoints)
3. [Sampling Methods](#sampling-methods)
4. [Multi-Round Sampling](#multi-round-sampling)
5. [Residual Datasets](#residual-datasets)
6. [Advanced Filtering](#advanced-filtering)
7. [Data Retrieval](#data-retrieval)
8. [Examples](#examples)
9. [Performance & Limits](#performance--limits)
10. [Security Notes](#security-notes)

## Overview

The DSA sampling system provides:
- **Multiple sampling methods**: Random, Stratified, Systematic, and Cluster sampling
- **Multi-round sampling**: Sequential sampling with different methods per round
- **Residual dataset export**: Capture unsampled rows for further analysis
- **Advanced filtering**: SQL-like expressions with AND/OR logic
- **Scalable architecture**: Handles datasets with 100M+ rows efficiently
- **Full audit trail**: Complete history and metadata tracking

## API Endpoints

### 1. Create Sampling Job
```
POST /api/sampling/datasets/{dataset_id}/jobs
```

Creates a multi-round sampling job with optional residual export. The system creates a single commit containing both sampled and residual data.

**Request Body:**
```json
{
  "source_ref": "main",
  "table_key": "primary",
  "output_name": "customer_analysis",
  "commit_message": "Sampled customer data from main branch",
  "rounds": [
    {
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 1000,
        "seed": 42
      },
      "output_name": "adults_nyc",
      "filters": {
        "expression": "age > 18 AND city = 'New York'"
      },
      "selection": {
        "columns": ["id", "name", "age", "city"],
        "order_by": "age",
        "order_desc": true
      }
    }
  ],
  "export_residual": true
}
```

**Notes:**
- `output_name` becomes the branch name with "smpl-" prefix (e.g., "smpl-customer_analysis")
- The commit contains two tables: "sample" (sampled data) and "residual" (unsampled data)
- `output_branch_name` and `residual_output_name` are deprecated

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Sampling job created with 1 rounds"
}
```

### 2. Get Available Sampling Methods
```
GET /api/sampling/datasets/{dataset_id}/sampling-methods
```

Returns available sampling methods with their parameter schemas.

**Response:**
```json
{
  "methods": [
    {
      "name": "random",
      "description": "Simple random sampling with optional seed for reproducibility",
      "parameters": [
        {
          "name": "sample_size",
          "type": "integer",
          "required": true,
          "description": "Number of samples"
        },
        {
          "name": "seed",
          "type": "integer",
          "required": false,
          "description": "Random seed"
        }
      ]
    },
    {
      "name": "stratified",
      "description": "Stratified sampling ensuring representation from all strata",
      "parameters": [
        {
          "name": "sample_size",
          "type": "integer",
          "required": true,
          "description": "Number of samples"
        },
        {
          "name": "seed",
          "type": "integer",
          "required": false,
          "description": "Random seed"
        },
        {
          "name": "strata_columns",
          "type": "array",
          "required": true,
          "description": "Columns to stratify by"
        },
        {
          "name": "min_per_stratum",
          "type": "integer",
          "required": false,
          "description": "Minimum samples per stratum"
        },
        {
          "name": "proportional",
          "type": "boolean",
          "required": false,
          "description": "Use proportional allocation"
        }
      ]
    },
    {
      "name": "systematic",
      "description": "Systematic sampling with fixed intervals",
      "parameters": [
        {
          "name": "interval",
          "type": "integer",
          "required": true,
          "description": "Sampling interval"
        },
        {
          "name": "start",
          "type": "integer",
          "required": false,
          "description": "Starting position"
        }
      ]
    },
    {
      "name": "cluster",
      "description": "Cluster sampling selecting entire groups",
      "parameters": [
        {
          "name": "cluster_column",
          "type": "string",
          "required": true,
          "description": "Column defining clusters"
        },
        {
          "name": "num_clusters",
          "type": "integer",
          "required": true,
          "description": "Number of clusters to select"
        },
        {
          "name": "samples_per_cluster",
          "type": "integer",
          "required": false,
          "description": "Samples per cluster"
        },
        {
          "name": "seed",
          "type": "integer",
          "required": false,
          "description": "Random seed"
        }
      ]
    },
    {
      "name": "reservoir",
      "description": "Reservoir sampling for memory-efficient sampling",
      "parameters": [
        {
          "name": "sample_size",
          "type": "integer",
          "required": true,
          "description": "Number of samples"
        },
        {
          "name": "seed",
          "type": "integer",
          "required": false,
          "description": "Random seed"
        }
      ]
    }
  ],
  "supported_operators": [
    ">", ">=", "<", "<=", "=", "!=", "in", "not_in",
    "like", "ilike", "is_null", "is_not_null"
  ]
}
```

### 3. Retrieve Sampled Data
```
GET /api/sampling/jobs/{job_id}/data
```

Retrieves sampled or residual data in JSON or CSV format.

**Query Parameters:**
- `format`: "json" (default) or "csv"
- `table_key`: "sample" (default) or "residual" for residual data
- `offset`: Pagination offset (default: 0)
- `limit`: Items per page (default: 100, max: 1000)
- `columns`: Comma-separated column names to include

**Alternative Access via Branch:**
```
GET /api/datasets/{dataset_id}/refs/smpl-{output_name}/tables/{table_key}/data
```
Where:
- `{output_name}` is from your sampling job request
- `{table_key}` is either "sample" or "residual"

**Response (JSON):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "dataset_id": 123,
  "commit_id": "commit_abc123",
  "table_key": "sample",
  "data": [
    {
      "id": 1,
      "name": "Alice",
      "age": 25,
      "city": "New York",
      "_logical_row_id": 12345
    }
  ],
  "pagination": {
    "total": 1000,
    "offset": 0,
    "limit": 100,
    "has_more": true
  },
  "metadata": {
    "sampling_summary": {
      "total_sampled": 1000,
      "rounds_executed": 1,
      "methods_used": ["random"]
    },
    "is_residual": false,
    "original_table_key": "sample",
    "round_details": [
      {
        "round_number": 1,
        "method": "random",
        "rows_sampled": 1000,
        "filters": "age > 18 AND city = 'New York'"
      }
    ],
    "residual_info": {
      "has_residual": true,
      "residual_count": 5000,
      "table_key": "residual"
    }
  },
  "columns": ["id", "name", "age", "city", "_logical_row_id"]
}
```

### 4. Get Sampling History

#### Dataset History
```
GET /api/sampling/datasets/{dataset_id}/history
```

**Query Parameters:**
- `ref_name`: Filter by ref name
- `status`: Filter by job status
- `start_date`: Filter jobs created after (ISO 8601)
- `end_date`: Filter jobs created before (ISO 8601)
- `offset`: Pagination offset (default: 0)
- `limit`: Items per page (default: 20, max: 100)

**Response:**
```json
{
  "dataset_id": 123,
  "dataset_name": "Customer Data",
  "jobs": [
    {
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "status": "completed",
      "created_at": "2025-01-02T10:00:00Z",
      "completed_at": "2025-01-02T10:05:00Z",
      "user_id": 456,
      "source_ref": "main",
      "output_branch_name": "smpl-customer_analysis",
      "rounds": 2,
      "total_sampled": 15000,
      "has_residual": true
    }
  ],
  "pagination": {
    "total": 50,
    "offset": 0,
    "limit": 20,
    "has_more": true
  }
}
```

#### User History
```
GET /api/sampling/users/{user_id}/history
```

**Query Parameters:**
- `dataset_id`: Filter by dataset
- `status`: Filter by job status
- `start_date`: Filter jobs created after (ISO 8601)
- `end_date`: Filter jobs created before (ISO 8601)
- `offset`: Pagination offset (default: 0)
- `limit`: Items per page (default: 20, max: 100)

**Response:**
```json
{
  "user_id": 456,
  "user_soeid": "alice",
  "user_name": "alice",
  "jobs": [
    {
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "dataset_id": 123,
      "dataset_name": "Customer Data",
      "status": "completed",
      "created_at": "2025-01-02T10:00:00Z",
      "completed_at": "2025-01-02T10:05:00Z",
      "source_ref": "main",
      "output_branch_name": "smpl-customer_analysis",
      "rounds": 2,
      "total_sampled": 15000,
      "has_residual": true
    }
  ],
  "pagination": {
    "total": 25,
    "offset": 0,
    "limit": 20,
    "has_more": true
  },
  "summary": {
    "total_sampling_jobs": 25,
    "successful_jobs": 23,
    "failed_jobs": 1,
    "total_rows_sampled": 350000,
    "datasets_sampled": 5
  }
}
```

## Sampling Methods

### 1. Random Sampling

Selects rows randomly from the dataset.

**Parameters:**
- `sample_size` (required): Number of rows to sample
- `seed` (optional): Random seed for reproducibility

**Modes:**
- **Unseeded**: Pure random selection, different results each run
- **Seeded**: Reproducible results using hash-based filtering

**Example:**
```json
{
  "method": "random",
  "parameters": {
    "sample_size": 10000,
    "seed": 42
  }
}
```

### 2. Stratified Sampling

Samples from different strata (groups) in the dataset.

**Parameters:**
- `strata_columns` (required): Columns defining strata
- `sample_size` OR `samples_per_stratum` (required): 
  - Use `sample_size` for proportional sampling
  - Use `samples_per_stratum` for disproportional (fixed-N) sampling
- `seed` (optional): Random seed for reproducibility
- `proportional` (optional): Boolean flag to use proportional allocation
- `min_per_stratum` (optional): Minimum samples per stratum

**Modes:**
- **Proportional**: Maintains original strata proportions
- **Disproportional**: Fixed number per stratum

**Example:**
```json
{
  "method": "stratified",
  "parameters": {
    "strata_columns": ["region", "age_group"],
    "sample_size": 5000,
    "proportional": true
  }
}
```

### 3. Systematic Sampling

Selects every nth row after a random start.

**Parameters:**
- `interval` (required): Sampling interval (e.g., every 10th row)
- `start` (optional): Starting position (default: random)

**Example:**
```json
{
  "method": "systematic",
  "parameters": {
    "interval": 100,
    "start": 5
  }
}
```

### 4. Cluster Sampling

Randomly selects clusters and samples within them.

**Parameters:**
- `cluster_column` (required): Column defining clusters
- `num_clusters` (required): Number of clusters to select
- `samples_per_cluster` (optional): Number of samples per cluster (if not specified, takes all rows from selected clusters)
- `seed` (optional): Random seed for reproducibility

**Example:**
```json
{
  "method": "cluster",
  "parameters": {
    "cluster_column": "store_id",
    "num_clusters": 50,
    "samples_per_cluster": 100,
    "seed": 42
  }
}
```

### 5. Reservoir Sampling

Memory-efficient sampling method for large datasets where the total size may not be known in advance.

**Parameters:**
- `sample_size` (required): Number of samples to collect
- `seed` (optional): Random seed for reproducibility

**Example:**
```json
{
  "method": "reservoir",
  "parameters": {
    "sample_size": 10000,
    "seed": 42
  }
}
```

## Multi-Round Sampling

Multi-round sampling allows sequential sampling with different methods and filters per round, ensuring no duplicates across rounds.

### How It Works

1. Each round is executed sequentially
2. Sampled rows are tracked in an exclusion table
3. Subsequent rounds automatically exclude previously sampled rows
4. Results from all rounds are combined

### Example: Three-Round Sampling

```json
{
  "source_ref": "main",
  "table_key": "primary",
  "output_name": "multi_round_analysis",
  "commit_message": "Multi-round sampling with residual export",
  "rounds": [
    {
      "round_number": 1,
      "method": "stratified",
      "parameters": {
        "strata_columns": ["priority"],
        "sample_size": 1000
      },
      "output_name": "high_priority_sample",
      "filters": {
        "expression": "priority = 'high'"
      }
    },
    {
      "round_number": 2,
      "method": "random",
      "parameters": {
        "sample_size": 2000,
        "seed": 123
      },
      "output_name": "recent_records_sample",
      "filters": {
        "expression": "created_date > '2024-01-01'"
      }
    },
    {
      "round_number": 3,
      "method": "cluster",
      "parameters": {
        "cluster_column": "department",
        "num_clusters": 5,
        "samples_per_cluster": 100
      },
      "output_name": "department_sample"
    }
  ],
  "export_residual": true
}
```

**Result:**
- Branch: `smpl-multi_round_analysis`
- Tables: `sample` (combined results from all rounds), `residual` (unsampled data)

## Residual Datasets

Residual datasets contain all rows not selected in any sampling round and are stored in the same commit as the sampled data.

### Enabling Residual Export

Simply set `export_residual: true` in your request:

```json
{
  "rounds": [...],
  "export_residual": true
}
```

### Accessing Residual Data

Option 1 - Via job endpoint:
```
GET /api/sampling/jobs/{job_id}/data?table_key=residual
```

Option 2 - Via branch endpoint:
```
GET /api/datasets/{dataset_id}/refs/smpl-{output_name}/tables/residual/data
```

### Use Cases

- **Quality assurance**: Verify sampling coverage
- **Iterative sampling**: Use residuals for subsequent sampling
- **Bias analysis**: Check characteristics of unsampled data
- **Completeness validation**: Ensure critical records are sampled

## Advanced Filtering

The system supports SQL-like filter expressions with full boolean logic.

### Supported Operators

- **Comparison**: `>`, `>=`, `<`, `<=`, `=`, `!=`
- **Set operations**: `IN`, `NOT IN`
- **Pattern matching**: `LIKE`, `ILIKE` (case-insensitive)
- **Null checks**: `IS NULL`, `IS NOT NULL`
- **Boolean logic**: `AND`, `OR` with parentheses

### Filter Examples

```python
# Simple filter
"age > 18"

# Multiple conditions
"age > 18 AND city = 'New York'"

# IN operator
"status IN ('active', 'pending')"

# Pattern matching
"email LIKE '%@company.com'"

# Complex logic with parentheses
"(age > 25 AND city = 'NYC') OR (age > 30 AND city = 'LA')"

# Null checks
"middle_name IS NOT NULL"

# Nested conditions
"(priority = 'high' OR (priority = 'medium' AND age > 30)) AND status != 'inactive'"
```

### Filter Limitations

- Maximum expression length: 1000 characters
- Maximum nesting depth: 10 levels
- Column names must exist in the dataset
- Values are parameterized to prevent SQL injection

## Data Retrieval

### JSON Format

Paginated JSON response with metadata:

```bash
curl -X GET "http://api.example.com/api/sampling/jobs/job_123/data?offset=0&limit=100" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### CSV Format

Streaming CSV download:

```bash
curl -X GET "http://api.example.com/api/sampling/jobs/job_123/data?format=csv&table_key=sample1" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -o sampled_data.csv
```

### Column Selection

When creating a job, specify `selected_columns` to retrieve only specific columns:

```json
{
  "selected_columns": ["id", "name", "email", "created_date"],
  "order_by": [
    {"column": "created_date", "direction": "DESC"}
  ]
}
```

## Examples

### Example 1: Customer Segmentation Sampling

Sample different customer segments with specific proportions:

```json
{
  "source_ref": "main",
  "table_key": "primary",
  "output_name": "customer_segments",
  "commit_message": "Customer segmentation sampling analysis",
  "rounds": [
    {
      "round_number": 1,
      "method": "stratified",
      "parameters": {
        "strata_columns": ["segment", "region"],
        "sample_size": 10000
      },
      "output_name": "active_customers",
      "filters": {
        "expression": "last_purchase_date > '2024-01-01'"
      },
      "selection": {
        "columns": ["customer_id", "segment", "region", "total_purchases"],
        "order_by": "total_purchases",
        "order_desc": true
      }
    },
    {
      "round_number": 2,
      "method": "random",
      "parameters": {
        "sample_size": 5000,
        "seed": 999
      },
      "output_name": "churned_customers",
      "filters": {
        "expression": "segment = 'inactive' AND total_purchases > 0"
      }
    }
  ],
  "export_residual": true
}
```

**Access:**
- Sampled data: `/api/datasets/{id}/refs/smpl-customer_segments/tables/sample/data`
- Residual data: `/api/datasets/{id}/refs/smpl-customer_segments/tables/residual/data`

### Example 2: Time-Based Systematic Sampling

Sample records at regular intervals over time:

```json
{
  "source_ref": "main",
  "table_key": "primary",
  "output_name": "time_series",
  "commit_message": "Time series systematic sampling",
  "rounds": [
    {
      "round_number": 1,
      "method": "systematic",
      "parameters": {
        "interval": 60,
        "start": 1
      },
      "output_name": "2024_time_sample",
      "filters": {
        "expression": "timestamp >= '2024-01-01' AND timestamp < '2025-01-01'"
      },
      "selection": {
        "order_by": "timestamp",
        "order_desc": false
      }
    }
  ]
}
```

### Example 3: Hierarchical Cluster Sampling

Sample stores and then customers within selected stores:

```json
{
  "source_ref": "main",
  "table_key": "primary",
  "output_name": "store_customers",
  "commit_message": "Hierarchical cluster sampling of retail stores",
  "rounds": [
    {
      "round_number": 1,
      "method": "cluster",
      "parameters": {
        "cluster_column": "store_id",
        "num_clusters": 100,
        "samples_per_cluster": 50
      },
      "output_name": "retail_store_customers",
      "filters": {
        "expression": "store_type = 'retail'"
      },
      "selection": {
        "columns": ["customer_id", "store_id", "purchase_amount"]
      }
    }
  ]
}
```

## Performance & Limits

### System Limits

- **Maximum sample size**: 1,000,000 rows per round
- **Maximum rounds**: No hard limit, but consider performance
- **Filter expression length**: 1,000 characters
- **Filter nesting depth**: 10 levels
- **Page size (JSON)**: 1,000 rows
- **Column name length**: 63 characters

### Performance Considerations

1. **Large Datasets (>100M rows)**:
   - Use seeded random sampling for better performance
   - Consider sampling in multiple smaller rounds
   - Stratified sampling may be slower on high-cardinality columns

2. **Optimization Tips**:
   - Index columns used in filters
   - Use specific filters to reduce scan size
   - For very large datasets, consider pre-filtering views

3. **Memory Usage**:
   - CSV export streams data (low memory)
   - JSON pagination prevents memory overload
   - Residual export uses anti-join (efficient for indexed columns)

## Security Notes

### Input Validation

- All column names validated against schema
- Filter expressions parsed and parameterized
- SQL injection prevention through whitelist operators
- Maximum limits enforced on all inputs

### Permissions

- Requires `read` permission on dataset
- Job creation requires `write` permission
- Results inherit dataset permissions
- Residual data access controlled separately

### Best Practices

1. **Always validate filters** before large sampling jobs
2. **Use seeds** for reproducible results in production
3. **Monitor job status** for long-running operations
4. **Test on small samples** before full-scale sampling
5. **Document sampling parameters** for audit trails

### Common Pitfalls to Avoid

1. **Over-sampling**: Don't sample more than 10% without good reason
2. **Complex filters on unindexed columns**: May cause timeouts
3. **Too many rounds**: Each round adds overhead
4. **Forgetting residuals**: Enable if you need complete coverage tracking
5. **Incorrect stratification columns**: Can lead to empty strata

## Troubleshooting

### Common Issues

1. **"Column not found" error**:
   - Check column exists in dataset schema
   - Verify column name case sensitivity

2. **"Invalid filter expression"**:
   - Check operator is supported
   - Verify parentheses are balanced
   - Ensure string values are quoted

3. **"Sample size too large"**:
   - Maximum is 1M rows per round
   - Consider multiple rounds or filtering

4. **Slow performance**:
   - Check if filter columns are indexed
   - Reduce sample size or simplify filters
   - Use seeded random for large datasets

5. **Empty results**:
   - Verify filters don't exclude all rows
   - Check dataset actually contains data
   - Ensure proper permissions

### Getting Help

For additional support:
- Check API response error messages
- Review job logs via the history endpoint
- Contact support with job_id for investigation