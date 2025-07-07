# UI Guide: Using DSA Sampling Endpoints

This guide helps frontend developers understand when and how to use each sampling endpoint in the DSA platform for building effective data exploration interfaces.

## Table of Contents
1. [Overview](#overview)
2. [Endpoint Reference](#endpoint-reference)
3. [UI Workflow Examples](#ui-workflow-examples)
4. [Best Practices](#best-practices)
5. [Error Handling](#error-handling)

## Overview

The DSA platform provides multiple sampling endpoints designed for different UI needs:
- **Quick filtering** - Get column values for dropdowns
- **Data preview** - Show sample data to users
- **Advanced sampling** - Create complex sampling jobs
- **History tracking** - Show past sampling operations

## Endpoint Reference

### 1. Get Available Sampling Methods
```
GET /api/sampling/datasets/{dataset_id}/sampling-methods
```

**When to use**: On sampling configuration screens to show available methods

**UI Implementation**:
```javascript
// Fetch available methods when user opens sampling dialog
const response = await fetch(`/api/sampling/datasets/${datasetId}/sampling-methods`, {
  headers: { 'Authorization': `Bearer ${token}` }
});
const data = await response.json();

// Populate method dropdown
const methodSelect = document.getElementById('sampling-method');
data.methods.forEach(method => {
  const option = new Option(method.description, method.name);
  methodSelect.add(option);
});

// Update parameter fields based on selected method
methodSelect.addEventListener('change', (e) => {
  const selectedMethod = data.methods.find(m => m.name === e.target.value);
  updateParameterFields(selectedMethod.parameters);
});
```

**Response Structure**:
```json
{
  "methods": [
    {
      "name": "random",
      "description": "Simple random sampling",
      "parameters": [
        {"name": "sample_size", "type": "integer", "required": true},
        {"name": "seed", "type": "integer", "required": false}
      ]
    },
    // ... other methods
  ]
}
```

### 2. Get Column Sample Values (For Filters)
```
POST /api/sampling/datasets/{dataset_id}/column-samples
```

**When to use**: Building filter dropdowns, autocomplete, or column value previews

**UI Implementation**:
```javascript
// Get unique values for filter dropdowns
async function loadFilterOptions(columns) {
  const response = await fetch(`/api/sampling/datasets/${datasetId}/column-samples`, {
    method: 'POST',
    headers: { 
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      columns: columns,  // e.g., ["Sales:region", "Sales:product_id"]
      sample_size: 20    // Max unique values per column
    })
  });
  
  const data = await response.json();
  
  // Populate filter dropdowns
  Object.entries(data.samples).forEach(([column, values]) => {
    const filterSelect = document.getElementById(`filter-${column}`);
    values.forEach(value => {
      filterSelect.add(new Option(value, value));
    });
  });
}

// For multi-sheet Excel files, use format "SheetName:columnName"
loadFilterOptions(["Sales:region", "Inventory:category", "Customers:loyalty_tier"]);
```

### 3. Quick Data Sampling (Synchronous)
```
POST /api/sampling/datasets/{dataset_id}/sample
```

**When to use**: Interactive data preview, quick sampling results

**UI Implementation**:
```javascript
// Preview data with specific sampling method
async function previewSample() {
  const samplingConfig = {
    method: "stratified",
    sample_size: 50,
    parameters: {
      strata_columns: ["region"],
      proportional: true
    },
    sheets: ["Sales", "Inventory"],  // Optional: specific sheets for Excel files
    filters: [  // Optional: pre-filter data
      {
        column: "Sales:region",
        operator: "in",
        value: ["North", "South"]
      }
    ]
  };

  const response = await fetch(`/api/sampling/datasets/${datasetId}/sample`, {
    method: 'POST',
    headers: { 
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(samplingConfig)
  });

  const result = await response.json();
  
  // Display sampled data in table
  displayDataTable(result.data);
  
  // Show sampling metadata
  showSamplingInfo({
    totalSampled: result.metadata.total_sampled,
    method: result.method,
    actualSampleSize: result.metadata.actual_sample_size
  });
}
```

### 4. Create Sampling Job (Asynchronous)
```
POST /api/sampling/datasets/{dataset_id}/jobs
```

**When to use**: Large sampling operations, multi-round sampling, creating new commits

**UI Implementation**:
```javascript
// Create sampling job for large datasets
async function createSamplingJob() {
  const jobConfig = {
    table_key: "Sales",  // For multi-sheet: specify sheet name
    source_ref: "main",
    create_output_commit: true,
    commit_message: "Stratified sample of Q1 sales data",
    rounds: [
      {
        round_number: 1,
        method: "stratified",
        sample_size: 1000,
        parameters: {
          strata_columns: ["region", "product_category"],
          proportional: true,
          min_per_stratum: 10
        },
        filters: [
          {
            column: "date",
            operator: ">=",
            value: "2024-01-01"
          }
        ]
      }
    ],
    export_residual: true  // Also save unsampled records
  };

  const response = await fetch(`/api/sampling/datasets/${datasetId}/jobs`, {
    method: 'POST',
    headers: { 
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(jobConfig)
  });

  const { job_id } = await response.json();
  
  // Poll for job completion
  pollJobStatus(job_id);
}

// Poll job status
async function pollJobStatus(jobId) {
  const checkStatus = setInterval(async () => {
    const response = await fetch(`/api/jobs/${jobId}`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    
    const job = await response.json();
    updateProgressBar(job.status, job.progress);
    
    if (job.status === 'completed') {
      clearInterval(checkStatus);
      showSuccessMessage('Sampling job completed!');
      // Optionally fetch the results
      fetchJobResults(jobId);
    } else if (job.status === 'failed') {
      clearInterval(checkStatus);
      showErrorMessage(job.error_message);
    }
  }, 2000); // Check every 2 seconds
}
```

### 5. Get Sampling Job Results
```
GET /api/sampling/jobs/{job_id}/data
GET /api/sampling/jobs/{job_id}/residual
```

**When to use**: Retrieve results after job completion

**UI Implementation**:
```javascript
// Fetch sampling job results
async function fetchJobResults(jobId) {
  // Get sampled data
  const sampledResponse = await fetch(`/api/sampling/jobs/${jobId}/data`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const sampledData = await sampledResponse.json();
  
  // Get residual (unsampled) data if needed
  const residualResponse = await fetch(`/api/sampling/jobs/${jobId}/residual`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const residualData = await residualResponse.json();
  
  // Update UI
  displayResults({
    sampled: sampledData.data,
    residual: residualData.data,
    metadata: sampledData.metadata
  });
}
```

### 6. View Sampling History
```
GET /api/sampling/datasets/{dataset_id}/history
GET /api/sampling/users/{user_id}/history
```

**When to use**: Show sampling history, reuse previous configurations

**UI Implementation**:
```javascript
// Show dataset sampling history
async function loadSamplingHistory() {
  const response = await fetch(`/api/sampling/datasets/${datasetId}/history`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  
  const { jobs } = await response.json();
  
  // Display history table
  const historyTable = document.getElementById('sampling-history');
  jobs.forEach(job => {
    const row = createHistoryRow({
      date: new Date(job.created_at).toLocaleDateString(),
      method: job.parameters.rounds[0].method,
      sampleSize: job.parameters.rounds[0].sample_size,
      status: job.status,
      jobId: job.id
    });
    historyTable.appendChild(row);
  });
}

// Allow reusing previous sampling configuration
function reuseSamplingConfig(jobId) {
  const job = jobs.find(j => j.id === jobId);
  populateSamplingForm(job.parameters);
}
```

## UI Workflow Examples

### Example 1: Data Exploration Interface
```javascript
// Complete data exploration workflow
class DataExplorer {
  constructor(datasetId, token) {
    this.datasetId = datasetId;
    this.token = token;
  }

  async initialize() {
    // 1. Load table analysis for overview
    const analysis = await this.getTableAnalysis();
    this.displayDataOverview(analysis);
    
    // 2. Load column samples for filters
    const columns = Object.keys(analysis.column_types);
    await this.loadFilterOptions(columns);
    
    // 3. Show initial data sample
    await this.showDataPreview();
  }

  async getTableAnalysis() {
    const response = await fetch(
      `/api/datasets/${this.datasetId}/refs/main/tables/primary/analysis`,
      { headers: { 'Authorization': `Bearer ${this.token}` }}
    );
    return response.json();
  }

  async showDataPreview() {
    const sample = await this.quickSample('random', 100);
    this.renderDataTable(sample.data);
  }

  async applyFilters(filters) {
    const sample = await this.quickSample('random', 100, filters);
    this.renderDataTable(sample.data);
  }
}
```

### Example 2: Multi-Sheet Excel Sampling UI
```javascript
// Handle multi-sheet Excel files
class MultiSheetSampler {
  constructor(datasetId, token) {
    this.datasetId = datasetId;
    this.token = token;
    this.sheets = [];
  }

  async loadSheets() {
    // Get schema to identify sheets
    const response = await fetch(
      `/api/datasets/${this.datasetId}/commits/HEAD/schema`,
      { headers: { 'Authorization': `Bearer ${this.token}` }}
    );
    const schema = await response.json();
    
    // Extract sheet names
    this.sheets = Object.values(schema.sheets).map(s => s.sheet_name);
    
    // Populate sheet selector
    const sheetSelect = document.getElementById('sheet-selector');
    this.sheets.forEach(sheet => {
      sheetSelect.add(new Option(sheet, sheet));
    });
  }

  async sampleSelectedSheets() {
    const selectedSheets = Array.from(
      document.getElementById('sheet-selector').selectedOptions
    ).map(opt => opt.value);

    const config = {
      method: "systematic",
      sample_size: 50,
      parameters: { interval: 5 },
      sheets: selectedSheets  // Sample only selected sheets
    };

    const response = await fetch(`/api/sampling/datasets/${this.datasetId}/sample`, {
      method: 'POST',
      headers: { 
        'Authorization': `Bearer ${this.token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(config)
    });

    const result = await response.json();
    this.displayMultiSheetResults(result);
  }
}
```

### Example 3: Advanced Sampling Configuration
```javascript
// Advanced sampling with multiple rounds
class AdvancedSampler {
  constructor(datasetId, token) {
    this.datasetId = datasetId;
    this.token = token;
    this.rounds = [];
  }

  addSamplingRound(config) {
    this.rounds.push({
      round_number: this.rounds.length + 1,
      ...config
    });
    this.updateRoundsDisplay();
  }

  async executeSampling() {
    const jobConfig = {
      table_key: document.getElementById('target-sheet').value,
      source_ref: "main",
      create_output_commit: true,
      commit_message: document.getElementById('commit-message').value,
      rounds: this.rounds,
      export_residual: document.getElementById('export-residual').checked
    };

    // Show progress modal
    this.showProgressModal();

    const response = await fetch(`/api/sampling/datasets/${this.datasetId}/jobs`, {
      method: 'POST',
      headers: { 
        'Authorization': `Bearer ${this.token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(jobConfig)
    });

    const { job_id } = await response.json();
    this.trackJobProgress(job_id);
  }

  async trackJobProgress(jobId) {
    const progressBar = document.getElementById('sampling-progress');
    
    const checkStatus = setInterval(async () => {
      const response = await fetch(`/api/jobs/${jobId}`, {
        headers: { 'Authorization': `Bearer ${this.token}` }
      });
      
      const job = await response.json();
      
      // Update progress bar
      progressBar.style.width = `${job.progress || 0}%`;
      progressBar.textContent = `${job.status}: ${job.progress || 0}%`;
      
      if (job.status === 'completed') {
        clearInterval(checkStatus);
        this.onSamplingComplete(jobId);
      } else if (job.status === 'failed') {
        clearInterval(checkStatus);
        this.onSamplingError(job.error_message);
      }
    }, 1000);
  }
}
```

## Best Practices

### 1. Performance Optimization
```javascript
// Cache column samples to avoid repeated API calls
class FilterCache {
  constructor() {
    this.cache = new Map();
  }

  async getColumnSamples(datasetId, columns, token) {
    const cacheKey = `${datasetId}-${columns.join(',')}`;
    
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey);
    }

    const response = await fetch(`/api/sampling/datasets/${datasetId}/column-samples`, {
      method: 'POST',
      headers: { 
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ columns, sample_size: 20 })
    });

    const data = await response.json();
    this.cache.set(cacheKey, data);
    
    // Expire cache after 5 minutes
    setTimeout(() => this.cache.delete(cacheKey), 5 * 60 * 1000);
    
    return data;
  }
}
```

### 2. Error Handling
```javascript
// Comprehensive error handling
async function safeSample(datasetId, config, token) {
  try {
    const response = await fetch(`/api/sampling/datasets/${datasetId}/sample`, {
      method: 'POST',
      headers: { 
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(config)
    });

    if (!response.ok) {
      const error = await response.json();
      
      // Handle specific errors
      switch (response.status) {
        case 400:
          if (error.detail.includes('Sample size exceeds')) {
            showWarning('Sample size too large. Maximum allowed: ' + extractMaxSize(error.detail));
            return null;
          }
          break;
        case 422:
          if (error.detail.includes('Invalid column')) {
            showError('Invalid column name. Please check your filters.');
            return null;
          }
          break;
        case 500:
          showError('Server error. Please try again later.');
          return null;
      }
      
      throw new Error(error.detail || 'Sampling failed');
    }

    return await response.json();
    
  } catch (error) {
    console.error('Sampling error:', error);
    showError('Failed to sample data: ' + error.message);
    return null;
  }
}
```

### 3. User Experience Enhancements
```javascript
// Provide real-time feedback
class SamplingUI {
  constructor() {
    this.setupEventListeners();
  }

  setupEventListeners() {
    // Update sample size estimate as user types
    document.getElementById('sample-percentage').addEventListener('input', async (e) => {
      const percentage = parseFloat(e.target.value);
      const totalRows = await this.getTotalRows();
      const estimatedSize = Math.floor(totalRows * percentage / 100);
      
      document.getElementById('estimated-size').textContent = 
        `Approximately ${estimatedSize.toLocaleString()} rows`;
    });

    // Validate stratification columns
    document.getElementById('stratify-columns').addEventListener('change', (e) => {
      const selected = Array.from(e.target.selectedOptions).map(o => o.value);
      
      if (selected.length > 3) {
        showWarning('Too many stratification columns may result in small strata');
      }
    });
  }

  // Show sampling method recommendations
  recommendSamplingMethod(dataCharacteristics) {
    const recommendations = [];
    
    if (dataCharacteristics.hasTimeColumn) {
      recommendations.push({
        method: 'systematic',
        reason: 'Good for time-series data to maintain temporal distribution'
      });
    }
    
    if (dataCharacteristics.hasCategoricalColumns) {
      recommendations.push({
        method: 'stratified',
        reason: 'Ensures representation from all categories'
      });
    }
    
    if (dataCharacteristics.hasNaturalClusters) {
      recommendations.push({
        method: 'cluster',
        reason: 'Efficient for data with natural groupings'
      });
    }
    
    return recommendations;
  }
}
```

### 4. Accessibility
```javascript
// Ensure sampling UI is accessible
class AccessibleSampler {
  constructor() {
    this.setupAccessibility();
  }

  setupAccessibility() {
    // Add ARIA labels
    document.getElementById('sampling-method').setAttribute(
      'aria-label', 
      'Select sampling method'
    );
    
    // Announce status updates
    this.statusAnnouncer = document.createElement('div');
    this.statusAnnouncer.setAttribute('role', 'status');
    this.statusAnnouncer.setAttribute('aria-live', 'polite');
    this.statusAnnouncer.className = 'sr-only';
    document.body.appendChild(this.statusAnnouncer);
  }

  announceStatus(message) {
    this.statusAnnouncer.textContent = message;
  }

  onSamplingComplete(result) {
    this.announceStatus(`Sampling complete. ${result.metadata.actual_sample_size} rows sampled.`);
  }
}
```

## Error Handling

### Common Error Scenarios

1. **Invalid Column Names** (422)
```javascript
// Handle column name errors for multi-sheet files
if (error.detail.includes('Column not found')) {
  showError('Column not found. For multi-sheet files, use format: SheetName:columnName');
}
```

2. **Sample Size Too Large** (400)
```javascript
// Gracefully handle size limits
if (error.detail.includes('exceeds total rows')) {
  const match = error.detail.match(/\d+/g);
  const maxSize = match ? match[match.length - 1] : 'unknown';
  showWarning(`Maximum sample size is ${maxSize} rows`);
}
```

3. **Authentication Errors** (401)
```javascript
// Handle token expiration
if (response.status === 401) {
  // Redirect to login or refresh token
  await refreshAuthToken();
  // Retry the request
  return retrySamplingRequest(config);
}
```

4. **Concurrent Modification** (409)
```javascript
// Handle concurrent dataset modifications
if (response.status === 409) {
  showWarning('Dataset has been modified. Please refresh and try again.');
  await refreshDatasetInfo();
}
```

## Summary

This guide provides comprehensive patterns for implementing sampling functionality in your UI:

1. Use **column-samples** for building interactive filters
2. Use **sample** endpoint for quick data previews
3. Use **jobs** endpoint for large operations and creating commits
4. Cache results where appropriate to improve performance
5. Provide clear feedback during long-running operations
6. Handle errors gracefully with user-friendly messages
7. Consider accessibility in your implementation

Remember to adapt these examples to your specific frontend framework (React, Vue, Angular, etc.) while maintaining the core patterns and best practices.