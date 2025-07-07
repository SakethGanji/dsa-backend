# Proper Sampling Flow for UI

## ❌ What You're Doing Wrong

You're calling:
1. `OPTIONS /api/datasets/48/refs/main/tables/Sales/analyze` (analysis endpoint)
2. `/api/sampling/datasets/48/j` (incomplete URL?)

These are NOT the correct endpoints for sampling!

## ✅ Correct Flow for Sampling

### Step 1: Create a Sampling Job

**Endpoint:**
```
POST /api/sampling/datasets/{dataset_id}/jobs
```

**Full Example:**
```bash
curl -X POST 'http://localhost:8000/api/sampling/datasets/48/jobs' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer YOUR_TOKEN' \
  -d '{
    "source_ref": "main",
    "table_key": "primary",
    "create_output_commit": true,
    "rounds": [
      {
        "round_number": 1,
        "method": "random",
        "parameters": {
          "sample_size": 1000
        }
      }
    ]
  }'
```

**Response:**
```json
{
  "job_id": "917f045a-ec63-47a1-b192-35149508f452",
  "status": "pending",
  "message": "Sampling job created with 1 rounds"
}
```

### Step 2: Check Job Status

**Endpoint:**
```
GET /api/jobs/{job_id}
```

**Example:**
```bash
curl 'http://localhost:8000/api/jobs/917f045a-ec63-47a1-b192-35149508f452' \
  -H 'Authorization: Bearer YOUR_TOKEN'
```

**Response:**
```json
{
  "id": "917f045a-ec63-47a1-b192-35149508f452",
  "status": "running",  // or "completed", "failed"
  "run_type": "sampling",
  "dataset_id": 48,
  // ... other fields
}
```

### Step 3: Get Sampled Data (After Completion)

**Endpoint:**
```
GET /api/sampling/jobs/{job_id}/data
```

**Example:**
```bash
curl 'http://localhost:8000/api/sampling/jobs/917f045a-ec63-47a1-b192-35149508f452/data?limit=100' \
  -H 'Authorization: Bearer YOUR_TOKEN'
```

## Complete JavaScript Example

```javascript
// Step 1: Create sampling job
async function createSamplingJob(datasetId) {
  const response = await fetch(`http://localhost:8000/api/sampling/datasets/${datasetId}/jobs`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${getAuthToken()}`
    },
    body: JSON.stringify({
      source_ref: "main",
      table_key: "primary",  // or "Sales" if that's your table name
      create_output_commit: true,
      rounds: [
        {
          round_number: 1,
          method: "random",
          parameters: {
            sample_size: 1000  // REQUIRED!
          }
        }
      ]
    })
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || 'Failed to create sampling job');
  }

  const result = await response.json();
  return result.job_id;
}

// Step 2: Poll for job completion
async function waitForJobCompletion(jobId) {
  while (true) {
    const response = await fetch(`http://localhost:8000/api/jobs/${jobId}`, {
      headers: {
        'Authorization': `Bearer ${getAuthToken()}`
      }
    });

    if (!response.ok) {
      throw new Error('Failed to check job status');
    }

    const job = await response.json();
    
    if (job.status === 'completed') {
      return job;
    } else if (job.status === 'failed') {
      throw new Error(job.error_message || 'Job failed');
    }
    
    // Wait 5 seconds before checking again
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
}

// Step 3: Get the sampled data
async function getSampledData(jobId) {
  const response = await fetch(`http://localhost:8000/api/sampling/jobs/${jobId}/data?limit=1000`, {
    headers: {
      'Authorization': `Bearer ${getAuthToken()}`
    }
  });

  if (!response.ok) {
    throw new Error('Failed to get sampled data');
  }

  return await response.json();
}

// Complete flow
async function performSampling(datasetId) {
  try {
    // Create job
    console.log('Creating sampling job...');
    const jobId = await createSamplingJob(datasetId);
    console.log(`Job created: ${jobId}`);
    
    // Wait for completion
    console.log('Waiting for job to complete...');
    const completedJob = await waitForJobCompletion(jobId);
    console.log('Job completed!');
    
    // Get data
    console.log('Fetching sampled data...');
    const data = await getSampledData(jobId);
    console.log(`Retrieved ${data.data.length} sampled rows`);
    
    return data;
  } catch (error) {
    console.error('Sampling failed:', error);
    throw error;
  }
}

// Usage
performSampling(48).then(data => {
  console.log('Sampling complete!', data);
});
```

## React Component Example

```jsx
import React, { useState } from 'react';

function SamplingComponent({ datasetId }) {
  const [loading, setLoading] = useState(false);
  const [jobId, setJobId] = useState(null);
  const [status, setStatus] = useState('');
  const [error, setError] = useState(null);
  const [sampledData, setSampledData] = useState(null);

  const startSampling = async () => {
    setLoading(true);
    setError(null);
    
    try {
      // Step 1: Create job
      setStatus('Creating sampling job...');
      const createResponse = await fetch(`/api/sampling/datasets/${datasetId}/jobs`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('authToken')}`
        },
        body: JSON.stringify({
          source_ref: "main",
          table_key: "primary",
          rounds: [{
            round_number: 1,
            method: "random",
            parameters: {
              sample_size: 1000
            }
          }]
        })
      });

      if (!createResponse.ok) {
        throw new Error('Failed to create sampling job');
      }

      const { job_id } = await createResponse.json();
      setJobId(job_id);
      
      // Step 2: Poll for status
      setStatus('Job running...');
      let job;
      do {
        await new Promise(resolve => setTimeout(resolve, 3000));
        
        const statusResponse = await fetch(`/api/jobs/${job_id}`, {
          headers: {
            'Authorization': `Bearer ${localStorage.getItem('authToken')}`
          }
        });
        
        job = await statusResponse.json();
        setStatus(`Job ${job.status}...`);
        
      } while (job.status === 'pending' || job.status === 'running');
      
      if (job.status === 'failed') {
        throw new Error(job.error_message || 'Sampling failed');
      }
      
      // Step 3: Get data
      setStatus('Fetching sampled data...');
      const dataResponse = await fetch(`/api/sampling/jobs/${job_id}/data`, {
        headers: {
          'Authorization': `Bearer ${localStorage.getItem('authToken')}`
        }
      });
      
      const data = await dataResponse.json();
      setSampledData(data);
      setStatus('Sampling complete!');
      
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <button onClick={startSampling} disabled={loading}>
        Start Sampling
      </button>
      
      {status && <p>Status: {status}</p>}
      {jobId && <p>Job ID: {jobId}</p>}
      {error && <p style={{color: 'red'}}>Error: {error}</p>}
      
      {sampledData && (
        <div>
          <h3>Sampled Data ({sampledData.data.length} rows)</h3>
          {/* Display your data here */}
        </div>
      )}
    </div>
  );
}
```

## Common Mistakes

### ❌ Wrong Endpoints
- `/api/datasets/{id}/refs/main/tables/Sales/analyze` - This is for analysis, not sampling
- `/api/sampling/datasets/{id}/j` - Incomplete URL
- `/api/datasets/{id}/sample` - This doesn't exist

### ✅ Correct Endpoints
- `POST /api/sampling/datasets/{dataset_id}/jobs` - Create sampling job
- `GET /api/jobs/{job_id}` - Check job status
- `GET /api/sampling/jobs/{job_id}/data` - Get sampled data

### ❌ Missing Required Parameters
```json
// WRONG - Missing sample_size
{
  "rounds": [{
    "method": "random",
    "parameters": {}
  }]
}
```

### ✅ Include Required Parameters
```json
// CORRECT
{
  "rounds": [{
    "round_number": 1,
    "method": "random",
    "parameters": {
      "sample_size": 1000
    }
  }]
}
```

## Quick Reference

1. **Create Job**: `POST /api/sampling/datasets/{dataset_id}/jobs`
2. **Check Status**: `GET /api/jobs/{job_id}`
3. **Get Data**: `GET /api/sampling/jobs/{job_id}/data`

That's it! Three simple steps.