# Sampling API Documentation

## Overview

The Sampling API provides endpoints for performing various sampling operations on dataset tables. It supports both synchronous (direct) and asynchronous (job-based) sampling with multiple sampling methods.

## Base URL

```
/api/sampling
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:

```
Authorization: Bearer <your-token>
```

## Endpoints

### 1. Create Sampling Job

Create an asynchronous sampling job that will be processed by the background worker.

**Endpoint:** `POST /api/sampling/datasets/{dataset_id}/jobs`

**Request Body:**

```json
{
  "source_ref": "main",
  "table_key": "primary",
  "create_output_commit": true,
  "commit_message": "Sampled data for analysis",
  "rounds": [
    {
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 1000,
        "seed": 42
      },
      "output_name": "Random Sample Round 1",
      "filters": {
        "conditions": [
          {
            "column": "age",
            "operator": ">",
            "value": 18
          },
          {
            "column": "status",
            "operator": "in",
            "value": ["active", "pending"]
          }
        ],
        "logic": "AND"
      },
      "selection": {
        "columns": ["id", "name", "age", "email"],
        "order_by": "created_at",
        "order_desc": true
      }
    },
    {
      "round_number": 2,
      "method": "stratified",
      "parameters": {
        "strata_columns": ["department", "region"],
        "sample_size": 500,
        "min_per_stratum": 10,
        "seed": 42
      },
      "output_name": "Stratified Sample by Department and Region"
    }
  ],
  "export_residual": true,
  "residual_output_name": "Remaining Records"
}
```

**Response:**

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Sampling job created with 2 rounds"
}
```

### 2. Direct Sampling

Perform sampling and return results immediately (synchronous).

**Endpoint:** `POST /api/sampling/datasets/{dataset_id}/sample`

**Query Parameters:**
- `ref_name` (string, default: "main"): Reference/branch name
- `table_key` (string, default: "primary"): Table to sample from

**Request Body:**

```json
{
  "method": "random",
  "sample_size": 100,
  "random_seed": 42,
  "offset": 0,
  "limit": 100,
  "filters": {
    "conditions": [
      {
        "column": "value",
        "operator": ">=",
        "value": 1000
      }
    ],
    "logic": "AND"
  }
}
```

**Response:**

```json
{
  "method": "random",
  "sample_size": 100,
  "data": [
    {
      "_logical_row_id": "primary:42",
      "id": 42,
      "name": "John Doe",
      "value": 1500
    }
    // ... more rows
  ],
  "metadata": {
    "commit_id": "abc123...",
    "table_key": "primary",
    "total_sampled": 100,
    "offset": 0,
    "limit": 100,
    "returned": 100
  }
}
```

### 3. Get Column Samples

Get unique value samples for specified columns (useful for UI filters).

**Endpoint:** `POST /api/sampling/datasets/{dataset_id}/column-samples`

**Query Parameters:**
- `ref_name` (string, default: "main"): Reference/branch name
- `table_key` (string, default: "primary"): Table to sample from

**Request Body:**

```json
{
  "columns": ["region", "department", "status"],
  "samples_per_column": 20
}
```

**Response:**

```json
{
  "samples": {
    "region": ["North", "South", "East", "West"],
    "department": ["Sales", "Marketing", "Engineering", "HR"],
    "status": ["active", "inactive", "pending"]
  },
  "metadata": {
    "dataset_id": 123,
    "ref_name": "main",
    "table_key": "primary",
    "commit_id": "abc123...",
    "columns_requested": 3,
    "samples_per_column": 20
  }
}
```

### 4. Get Available Sampling Methods

Get information about available sampling methods and their parameters.

**Endpoint:** `GET /api/sampling/datasets/{dataset_id}/sampling-methods`

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
        }
      ]
    }
    // ... more methods
  ],
  "supported_operators": [
    ">", ">=", "<", "<=", "=", "!=", 
    "in", "not_in", "like", "ilike", 
    "is_null", "is_not_null"
  ]
}
```

## Sampling Methods

### 1. Random Sampling

```json
{
  "method": "random",
  "parameters": {
    "sample_size": 1000,
    "seed": 42  // Optional, for reproducibility
  }
}
```

### 2. Stratified Sampling

```json
{
  "method": "stratified",
  "parameters": {
    "sample_size": 1000,
    "strata_columns": ["region", "category"],
    "min_per_stratum": 10,
    "proportional": true,
    "seed": 42
  }
}
```

### 3. Systematic Sampling

```json
{
  "method": "systematic",
  "parameters": {
    "interval": 10,
    "start": 1
  }
}
```

### 4. Cluster Sampling

```json
{
  "method": "cluster",
  "parameters": {
    "cluster_column": "department_id",
    "num_clusters": 5,
    "samples_per_cluster": 100,  // Or use sample_percentage
    "seed": 42
  }
}
```

## Filter Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `>` | Greater than | `{"column": "age", "operator": ">", "value": 18}` |
| `>=` | Greater than or equal | `{"column": "score", "operator": ">=", "value": 80}` |
| `<` | Less than | `{"column": "price", "operator": "<", "value": 100}` |
| `<=` | Less than or equal | `{"column": "quantity", "operator": "<=", "value": 10}` |
| `=` | Equal to | `{"column": "status", "operator": "=", "value": "active"}` |
| `!=` | Not equal to | `{"column": "type", "operator": "!=", "value": "test"}` |
| `in` | In list | `{"column": "region", "operator": "in", "value": ["North", "South"]}` |
| `not_in` | Not in list | `{"column": "category", "operator": "not_in", "value": ["A", "B"]}` |
| `like` | Pattern match | `{"column": "name", "operator": "like", "value": "John%"}` |
| `ilike` | Case-insensitive pattern | `{"column": "email", "operator": "ilike", "value": "%@gmail.com"}` |
| `is_null` | Is null | `{"column": "deleted_at", "operator": "is_null"}` |
| `is_not_null` | Is not null | `{"column": "email", "operator": "is_not_null"}` |

## Example Use Cases

### 1. Quality Assurance Sampling

Sample 5% of records from each product category for quality review:

```json
{
  "rounds": [
    {
      "round_number": 1,
      "method": "stratified",
      "parameters": {
        "strata_columns": ["product_category"],
        "sample_size": 5000,
        "proportional": true,
        "seed": 12345
      },
      "output_name": "QA Sample - 5% per category"
    }
  ]
}
```

### 2. A/B Testing Sample

Create treatment and control groups:

```json
{
  "rounds": [
    {
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 10000,
        "seed": 1
      },
      "output_name": "Treatment Group",
      "filters": {
        "conditions": [
          {"column": "signup_date", "operator": ">=", "value": "2024-01-01"}
        ]
      }
    },
    {
      "round_number": 2,
      "method": "random",
      "parameters": {
        "sample_size": 10000,
        "seed": 2
      },
      "output_name": "Control Group"
    }
  ],
  "export_residual": true,
  "residual_output_name": "Not in Experiment"
}
```

### 3. Geographic Cluster Sampling

Sample data from random geographic regions:

```json
{
  "rounds": [
    {
      "round_number": 1,
      "method": "cluster",
      "parameters": {
        "cluster_column": "zip_code",
        "num_clusters": 20,
        "sample_percentage": 10,
        "seed": 42
      },
      "output_name": "Geographic Sample - 20 ZIP codes"
    }
  ]
}
```

## Error Responses

### 400 Bad Request

```json
{
  "detail": "Invalid column name: user'; DROP TABLE--"
}
```

### 403 Forbidden

```json
{
  "detail": "No read permission on dataset"
}
```

### 404 Not Found

```json
{
  "detail": "Dataset not found"
}
```

### 422 Unprocessable Entity

```json
{
  "detail": [
    {
      "loc": ["body", "sample_size"],
      "msg": "ensure this value is greater than 0",
      "type": "value_error.number.not_gt"
    }
  ]
}
```

## Best Practices

1. **Use Seeds for Reproducibility**: Always provide a seed when you need consistent results
2. **Start Small**: Test with smaller sample sizes before running large sampling jobs
3. **Use Filters Wisely**: Apply filters to reduce the dataset before sampling
4. **Monitor Job Status**: For large sampling jobs, poll the job status endpoint
5. **Validate Columns**: Use the column-samples endpoint to verify column names exist
6. **Consider Performance**: Stratified sampling on high-cardinality columns can be slow