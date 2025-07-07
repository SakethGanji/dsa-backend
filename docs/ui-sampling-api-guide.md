# UI Sampling API Guide

This guide explains how the UI should correctly send sampling requests to the backend API.

## Endpoint

```
POST /api/sampling/datasets/{dataset_id}/jobs
```

## Request Structure

### Basic Request Format

```json
{
  "source_ref": "main",           // Branch/ref to sample from (default: "main")
  "table_key": "primary",         // Table to sample (default: "primary")
  "create_output_commit": true,   // Whether to create a new commit with results
  "commit_message": "Custom message", // Optional commit message
  "rounds": [                     // Array of sampling rounds
    {
      "round_number": 1,
      "method": "random",
      "parameters": {
        // Method-specific parameters (REQUIRED)
      },
      "output_name": "Sample 1",  // Optional name for this round
      "filters": {                // Optional row filters
        "conditions": [...],
        "logic": "AND"
      },
      "selection": {              // Optional column selection
        "columns": ["col1", "col2"],
        "order_by": "col1",
        "order_desc": false
      }
    }
  ],
  "export_residual": false,       // Export unsampled records
  "residual_output_name": "Remaining Data" // Name for residual data
}
```

## Sampling Methods and Required Parameters

### 1. Random Sampling

**Method**: `"random"`

**Required Parameters**:
- `sample_size` (integer, > 0): Number of rows to sample

**Optional Parameters**:
- `seed` (integer): Random seed for reproducibility

**Example**:
```json
{
  "round_number": 1,
  "method": "random",
  "parameters": {
    "sample_size": 1000,    // REQUIRED
    "seed": 42              // Optional
  }
}
```

### 2. Stratified Sampling

**Method**: `"stratified"`

**Required Parameters**:
- `sample_size` (integer, > 0): Total number of rows to sample
- `strata_columns` (array of strings): Columns to stratify by

**Optional Parameters**:
- `min_per_stratum` (integer): Minimum samples per stratum
- `proportional` (boolean): Use proportional allocation (default: true)
- `seed` (integer): Random seed

**Example**:
```json
{
  "round_number": 1,
  "method": "stratified",
  "parameters": {
    "sample_size": 1000,              // REQUIRED
    "strata_columns": ["region", "category"], // REQUIRED
    "min_per_stratum": 10,            // Optional
    "proportional": true,             // Optional
    "seed": 42                        // Optional
  }
}
```

### 3. Systematic Sampling

**Method**: `"systematic"`

**Required Parameters**:
- `interval` (integer, > 0): Sampling interval (e.g., every nth row)

**Optional Parameters**:
- `start` (integer): Starting position (default: 1)

**Example**:
```json
{
  "round_number": 1,
  "method": "systematic",
  "parameters": {
    "interval": 10,         // REQUIRED - sample every 10th row
    "start": 5              // Optional - start from 5th row
  }
}
```

### 4. Cluster Sampling

**Method**: `"cluster"`

**Required Parameters**:
- `cluster_column` (string): Column that defines clusters
- `num_clusters` (integer, > 0): Number of clusters to select

**Optional Parameters**:
- `samples_per_cluster` (integer): Fixed number of samples per cluster
- `sample_percentage` (float): Percentage of rows to sample from each cluster
- `seed` (integer): Random seed

**Example**:
```json
{
  "round_number": 1,
  "method": "cluster",
  "parameters": {
    "cluster_column": "store_id",     // REQUIRED
    "num_clusters": 20,               // REQUIRED
    "samples_per_cluster": 100,       // Optional (use this OR sample_percentage)
    "seed": 42                        // Optional
  }
}
```

## Complete Examples

### Example 1: Simple Random Sampling

```json
{
  "source_ref": "main",
  "table_key": "primary",
  "create_output_commit": true,
  "commit_message": "Random sample of 1000 rows",
  "rounds": [
    {
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 1000,
        "seed": 12345
      },
      "output_name": "Random Sample"
    }
  ]
}
```

### Example 2: Multi-Round Sampling with Different Methods

```json
{
  "source_ref": "main",
  "table_key": "primary",
  "create_output_commit": true,
  "commit_message": "Multi-round sampling: stratified + random",
  "rounds": [
    {
      "round_number": 1,
      "method": "stratified",
      "parameters": {
        "sample_size": 500,
        "strata_columns": ["category"],
        "min_per_stratum": 50
      },
      "output_name": "Stratified by Category"
    },
    {
      "round_number": 2,
      "method": "random",
      "parameters": {
        "sample_size": 300,
        "seed": 999
      },
      "output_name": "Additional Random Sample"
    }
  ],
  "export_residual": true,
  "residual_output_name": "Unsampled Records"
}
```

### Example 3: Sampling with Filters and Column Selection

```json
{
  "source_ref": "main",
  "table_key": "primary",
  "create_output_commit": true,
  "rounds": [
    {
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 1000
      },
      "filters": {
        "conditions": [
          {
            "column": "price",
            "operator": ">",
            "value": 100
          },
          {
            "column": "category",
            "operator": "in",
            "value": ["Electronics", "Clothing"]
          }
        ],
        "logic": "AND"
      },
      "selection": {
        "columns": ["id", "name", "price", "category"],
        "order_by": "price",
        "order_desc": true
      }
    }
  ]
}
```

## UI Implementation Guidelines

### 1. Form Validation

The UI should validate parameters before sending the request:

```javascript
// Example validation for random sampling
function validateRandomSampling(params) {
  if (!params.sample_size) {
    return { valid: false, error: "Sample size is required" };
  }
  if (params.sample_size <= 0) {
    return { valid: false, error: "Sample size must be positive" };
  }
  if (params.seed && !Number.isInteger(params.seed)) {
    return { valid: false, error: "Seed must be an integer" };
  }
  return { valid: true };
}
```

### 2. Dynamic Form Fields

Show/hide fields based on selected method:

```javascript
const methodFields = {
  random: {
    required: ['sample_size'],
    optional: ['seed']
  },
  stratified: {
    required: ['sample_size', 'strata_columns'],
    optional: ['min_per_stratum', 'proportional', 'seed']
  },
  systematic: {
    required: ['interval'],
    optional: ['start']
  },
  cluster: {
    required: ['cluster_column', 'num_clusters'],
    optional: ['samples_per_cluster', 'sample_percentage', 'seed']
  }
};
```

### 3. Default Values

Provide sensible defaults:

```javascript
const defaultParams = {
  random: {
    sample_size: 1000
  },
  stratified: {
    sample_size: 1000,
    proportional: true,
    min_per_stratum: 1
  },
  systematic: {
    interval: 10,
    start: 1
  },
  cluster: {
    num_clusters: 10
  }
};
```

### 4. Error Handling

Handle API validation errors:

```javascript
try {
  const response = await fetch(`/api/sampling/datasets/${datasetId}/jobs`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`
    },
    body: JSON.stringify(requestBody)
  });

  if (response.status === 422) {
    const error = await response.json();
    // Show validation error to user
    console.error('Validation error:', error.detail);
  }
} catch (error) {
  console.error('Request failed:', error);
}
```

## Common Mistakes to Avoid

1. **Empty parameters object**: Always include required parameters
   ```json
   // ❌ Wrong
   { "method": "random", "parameters": {} }
   
   // ✅ Correct
   { "method": "random", "parameters": { "sample_size": 1000 } }
   ```

2. **Wrong parameter types**: Ensure correct data types
   ```json
   // ❌ Wrong
   { "sample_size": "1000" }  // String instead of number
   
   // ✅ Correct
   { "sample_size": 1000 }    // Number
   ```

3. **Missing required fields for specific methods**:
   ```json
   // ❌ Wrong - stratified without strata_columns
   { "method": "stratified", "parameters": { "sample_size": 1000 } }
   
   // ✅ Correct
   { "method": "stratified", "parameters": { 
     "sample_size": 1000, 
     "strata_columns": ["region"] 
   }}
   ```

## Response Format

Successful response:
```json
{
  "job_id": "917f045a-ec63-47a1-b192-35149508f452",
  "status": "pending",
  "message": "Sampling job created with 1 rounds"
}
```

Validation error (422):
```json
{
  "detail": [
    {
      "loc": ["body", "rounds", 0, "parameters"],
      "msg": "Random sampling requires 'sample_size' parameter",
      "type": "value_error"
    }
  ]
}
```

## Testing Your Implementation

Use this checklist:
- [ ] All sampling methods have required parameters
- [ ] Parameter types are correct (numbers are numbers, not strings)
- [ ] Round numbers start from 1
- [ ] Filters use valid operators
- [ ] Column names in filters/selection exist in the dataset
- [ ] Sample size is reasonable for the dataset size