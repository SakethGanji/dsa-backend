# UI Sampling Implementation Checklist

## 🔍 Quick Validation Checklist

Use this checklist to verify your sampling implementation is correct.

## 1. Creating Sampling Jobs

### ✅ Endpoint
```
POST /api/sampling/datasets/{dataset_id}/jobs
```

### ✅ Required Headers
```javascript
headers: {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer {token}'
}
```

### ✅ Request Body Structure

#### ❌ WRONG - What NOT to do:
```json
{
  "rounds": [{
    "method": "random",
    "parameters": {}  // ❌ EMPTY PARAMETERS!
  }]
}
```

#### ✅ CORRECT - Minimal valid request:
```json
{
  "source_ref": "main",
  "table_key": "Sales",  // Must match actual table name
  "rounds": [{
    "round_number": 1,
    "method": "random",
    "parameters": {
      "sample_size": 1000  // ✅ REQUIRED!
    }
  }]
}
```

## 2. Required Parameters by Method

### 🎲 Random Sampling
```javascript
// CHECK: Do you have sample_size?
{
  "method": "random",
  "parameters": {
    "sample_size": 1000,  // ✅ REQUIRED (integer > 0)
    "seed": 42           // Optional (for reproducibility)
  }
}
```

### 📊 Stratified Sampling
```javascript
// CHECK: Do you have sample_size AND strata_columns?
{
  "method": "stratified",
  "parameters": {
    "sample_size": 1000,              // ✅ REQUIRED
    "strata_columns": ["region"],     // ✅ REQUIRED (non-empty array)
    "min_per_stratum": 5,            // Optional
    "proportional": true,            // Optional
    "seed": 123                      // Optional
  }
}
```

### 📏 Systematic Sampling
```javascript
// CHECK: Do you have interval?
{
  "method": "systematic",
  "parameters": {
    "interval": 10,  // ✅ REQUIRED (integer > 0)
    "start": 5      // Optional (default: 1)
  }
}
```

### 🎯 Cluster Sampling
```javascript
// CHECK: Do you have cluster_column AND num_clusters?
{
  "method": "cluster",
  "parameters": {
    "cluster_column": "product_id",   // ✅ REQUIRED
    "num_clusters": 5,               // ✅ REQUIRED (integer > 0)
    "samples_per_cluster": 10,       // Optional
    "seed": 456                      // Optional
  }
}
```

## 3. Job Status Checking

### ✅ Correct Flow
```javascript
// Step 1: Create job
const createResponse = await fetch('/api/sampling/datasets/48/jobs', {
  method: 'POST',
  headers: { /* ... */ },
  body: JSON.stringify({ /* ... */ })
});

const { job_id } = await createResponse.json();

// Step 2: Poll for completion
let jobComplete = false;
let jobData = null;

while (!jobComplete) {
  const statusResponse = await fetch(`/api/jobs/${job_id}`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  
  jobData = await statusResponse.json();
  
  // CHECK: Are you handling all statuses?
  switch (jobData.status) {
    case 'completed':
      jobComplete = true;
      break;
    case 'failed':
      throw new Error(jobData.error_message);
    case 'pending':
    case 'running':
      await new Promise(resolve => setTimeout(resolve, 3000));
      break;
  }
}
```

## 4. Data Retrieval

### ❌ WRONG - Common Mistake
```javascript
// Missing table_key parameter!
fetch(`/api/sampling/jobs/${job_id}/data`)
```

### ✅ CORRECT
```javascript
// CHECK: Are you using the same table_key from job creation?
const tableKey = jobData.run_parameters.table_key || 'primary';

fetch(`/api/sampling/jobs/${job_id}/data?table_key=${tableKey}&offset=0&limit=100`, {
  headers: { 'Authorization': `Bearer ${token}` }
})
```

## 5. Error Handling Checklist

### ✅ Parameter Validation Errors (422)
```javascript
if (response.status === 422) {
  const error = await response.json();
  // CHECK: Are you showing the specific validation error?
  // Example: "Random sampling requires 'sample_size' parameter"
  showError(error.detail[0].msg);
}
```

### ✅ Job Creation Errors
```javascript
const response = await createSamplingJob(/* ... */);
const data = await response.json();

// CHECK: Are you verifying job_id exists?
if (!data.job_id) {
  showError('Failed to create sampling job');
  return;
}
```

### ✅ Data Retrieval Errors
```javascript
const dataResponse = await fetch(/* ... */);

if (!dataResponse.ok) {
  const error = await dataResponse.json();
  // CHECK: Are you handling "Table 'primary' not found"?
  if (error.detail.includes('not found')) {
    showError(`Table ${tableKey} not found. Check your table_key parameter.`);
  }
}
```

## 6. Complete Implementation Example

```javascript
class SamplingService {
  async createAndRetrieveSample(datasetId, samplingConfig) {
    try {
      // 1. Validate parameters BEFORE sending
      this.validateParameters(samplingConfig);
      
      // 2. Create job with correct structure
      const jobId = await this.createSamplingJob(datasetId, samplingConfig);
      
      // 3. Wait for completion with timeout
      const job = await this.waitForJob(jobId, 60000); // 60 second timeout
      
      // 4. Retrieve data with correct table_key
      const data = await this.retrieveSampledData(jobId, samplingConfig.table_key);
      
      return data;
      
    } catch (error) {
      this.handleError(error);
    }
  }
  
  validateParameters(config) {
    // CHECK: Are you validating BEFORE sending to API?
    const { rounds, table_key } = config;
    
    if (!table_key) {
      throw new Error('table_key is required');
    }
    
    rounds.forEach((round, index) => {
      const { method, parameters } = round;
      
      switch (method) {
        case 'random':
          if (!parameters.sample_size || parameters.sample_size <= 0) {
            throw new Error(`Round ${index + 1}: sample_size is required for random sampling`);
          }
          break;
          
        case 'stratified':
          if (!parameters.sample_size || parameters.sample_size <= 0) {
            throw new Error(`Round ${index + 1}: sample_size is required`);
          }
          if (!parameters.strata_columns || parameters.strata_columns.length === 0) {
            throw new Error(`Round ${index + 1}: strata_columns is required`);
          }
          break;
          
        case 'systematic':
          if (!parameters.interval || parameters.interval <= 0) {
            throw new Error(`Round ${index + 1}: interval is required`);
          }
          break;
          
        case 'cluster':
          if (!parameters.cluster_column) {
            throw new Error(`Round ${index + 1}: cluster_column is required`);
          }
          if (!parameters.num_clusters || parameters.num_clusters <= 0) {
            throw new Error(`Round ${index + 1}: num_clusters is required`);
          }
          break;
      }
    });
  }
}
```

## 7. Testing Your Implementation

### Test Case 1: Random Sampling
```javascript
// CHECK: Does this work?
const test1 = {
  table_key: "Sales",
  rounds: [{
    round_number: 1,
    method: "random",
    parameters: { sample_size: 10, seed: 42 }
  }]
};
```

### Test Case 2: Missing Parameters
```javascript
// CHECK: Does this show an error?
const test2 = {
  table_key: "Sales",
  rounds: [{
    round_number: 1,
    method: "random",
    parameters: {} // Should fail!
  }]
};
```

### Test Case 3: Multi-round
```javascript
// CHECK: Does this create all rounds?
const test3 = {
  table_key: "Sales",
  rounds: [
    {
      round_number: 1,
      method: "random",
      parameters: { sample_size: 10 }
    },
    {
      round_number: 2,
      method: "systematic",
      parameters: { interval: 5 }
    }
  ]
};
```

## 8. Common Issues & Solutions

| Issue | Symptom | Solution |
|-------|---------|----------|
| Missing sample_size | Error: "'sample_size'" | Add `sample_size` to parameters |
| Wrong table_key | "Table 'primary' not found" | Use same table_key for creation and retrieval |
| No job polling | Data not ready | Implement status polling loop |
| Type mismatch | Validation errors | Ensure numbers are integers, not strings |
| Missing auth | 401 Unauthorized | Include Bearer token in all requests |

## 9. Debug Checklist

When something goes wrong, check:

- [ ] Is `sample_size` included for random/stratified?
- [ ] Is `table_key` the same in job creation and data retrieval?
- [ ] Are you polling job status before retrieving data?
- [ ] Are numeric parameters integers (not strings)?
- [ ] Is the Authorization header included?
- [ ] Are you handling all error responses?
- [ ] Is `round_number` starting from 1?
- [ ] Are array parameters actual arrays (not strings)?

## 10. Quick Test Script

```javascript
// Paste this in console to test your implementation
async function testSampling() {
  const token = 'YOUR_TOKEN';
  const datasetId = 48;
  
  // Test 1: Should succeed
  console.log('Test 1: Valid request');
  const response1 = await fetch(`/api/sampling/datasets/${datasetId}/jobs`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`
    },
    body: JSON.stringify({
      table_key: "Sales",
      rounds: [{
        round_number: 1,
        method: "random",
        parameters: { sample_size: 5 }
      }]
    })
  });
  console.log('Response:', await response1.json());
  
  // Test 2: Should fail with validation error
  console.log('\nTest 2: Missing sample_size');
  const response2 = await fetch(`/api/sampling/datasets/${datasetId}/jobs`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`
    },
    body: JSON.stringify({
      table_key: "Sales",
      rounds: [{
        round_number: 1,
        method: "random",
        parameters: {}
      }]
    })
  });
  console.log('Response:', response2.status, await response2.json());
}
```

## ✅ Final Verification

Your implementation is correct if:
1. Random sampling works with just `sample_size`
2. Invalid parameters show clear error messages
3. Job status updates from pending → running → completed
4. Data retrieval returns the expected number of rows
5. Multi-round sampling doesn't duplicate data

Use this guide to double-check your implementation!