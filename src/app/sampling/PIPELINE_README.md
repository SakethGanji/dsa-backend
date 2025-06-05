# Sampling Pipeline Feature

## Overview

The sampling pipeline feature allows you to compose multiple sampling steps in sequence, enabling complex data sampling workflows. Each step operates on the output of the previous step, creating a powerful and flexible sampling system.

## Available Pipeline Steps

### 1. **Filter** (`filter`)
Apply conditional filtering to the data.
- **Parameters:**
  - `conditions`: List of filter conditions
  - `logic`: "AND" or "OR" logic between conditions

### 2. **Stratified Sample** (`stratified_sample`)
Sample data proportionally across strata (groups).
- **Parameters:**
  - `strata_columns`: List of columns to stratify by
  - `sample_size`: Number or fraction of samples (optional)
  - `min_per_stratum`: Minimum samples per stratum (optional)
  - `seed`: Random seed for reproducibility (optional)

### 3. **Cluster Sample** (`cluster_sample`)
Sample entire clusters or groups of data.
- **Parameters:**
  - `cluster_column`: Column that identifies clusters
  - `num_clusters`: Number of clusters to sample
  - `sample_within_clusters`: Whether to sample within selected clusters (optional)

### 4. **Consecutive Sample** (`consecutive_sample`)
Take every nth record (systematic sampling).
- **Parameters:**
  - `interval`: Take every nth record
  - `start`: Starting index (default is 0)

### 5. **Random Sample** (`random_sample`)
Randomly sample records.
- **Parameters:**
  - `sample_size`: Number or fraction of samples
  - `seed`: Random seed for reproducibility (optional)

## Usage

### Pipeline Mode

Send a request with a `pipeline` array instead of `method` and `parameters`:

```json
{
  "pipeline": [
    {
      "step": "filter",
      "parameters": {
        "conditions": [
          {"column": "status", "operator": "=", "value": "active"}
        ],
        "logic": "AND"
      }
    },
    {
      "step": "random_sample",
      "parameters": {
        "sample_size": 1000,
        "seed": 42
      }
    }
  ],
  "output_name": "filtered_random_sample",
  "selection": {
    "columns": ["id", "name", "status"],
    "order_by": "created_date",
    "order_desc": true
  }
}
```

### Traditional Mode (Backward Compatible)

The original API still works:

```json
{
  "method": "stratified",
  "parameters": {
    "strata_columns": ["region"],
    "sample_size": 1000
  },
  "output_name": "stratified_sample",
  "filters": {
    "conditions": [
      {"column": "status", "operator": "=", "value": "active"}
    ],
    "logic": "AND"
  }
}
```

## Examples

### Example 1: Filter then Random Sample
```json
{
  "pipeline": [
    {
      "step": "filter",
      "parameters": {
        "conditions": [
          {"column": "score", "operator": ">", "value": 80}
        ],
        "logic": "AND"
      }
    },
    {
      "step": "random_sample",
      "parameters": {
        "sample_size": 0.1
      }
    }
  ],
  "output_name": "high_score_sample"
}
```

### Example 2: Complex Multi-Stage Pipeline
```json
{
  "pipeline": [
    {
      "step": "filter",
      "parameters": {
        "conditions": [
          {"column": "year", "operator": ">=", "value": 2023}
        ],
        "logic": "AND"
      }
    },
    {
      "step": "stratified_sample",
      "parameters": {
        "strata_columns": ["region", "category"],
        "sample_size": 0.5
      }
    },
    {
      "step": "cluster_sample",
      "parameters": {
        "cluster_column": "department",
        "num_clusters": 5
      }
    },
    {
      "step": "consecutive_sample",
      "parameters": {
        "interval": 10
      }
    },
    {
      "step": "random_sample",
      "parameters": {
        "sample_size": 100
      }
    }
  ],
  "output_name": "complex_sample"
}
```

## Key Features

1. **Composable**: Mix and match any combination of steps
2. **Order Matters**: Steps are executed in the order specified
3. **Without Replacement**: Each step samples without replacement
4. **Efficient**: Uses DuckDB views for memory-efficient processing
5. **Flexible**: Supports both absolute numbers and fractions for sample sizes

## Implementation Details

- Pipeline steps are executed sequentially using DuckDB temporary views
- Each step creates a new view based on the previous step's output
- Final selection and ordering are applied after all pipeline steps
- Memory-efficient: Data is not materialized until the final step
- Supports large datasets through streaming processing

## API Endpoints

### Create Pipeline Sampling Job
```
POST /api/sampling/{dataset_id}/{version_id}/run
```

### Execute Pipeline Synchronously
```
POST /api/sampling/{dataset_id}/{version_id}/execute
```

Both endpoints accept the same request format and will automatically detect pipeline mode based on the presence of the `pipeline` field.