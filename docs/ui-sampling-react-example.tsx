import React, { useState } from 'react';

// Type definitions
interface SamplingParameters {
  sample_size?: number;
  seed?: number;
  strata_columns?: string[];
  min_per_stratum?: number;
  proportional?: boolean;
  interval?: number;
  start?: number;
  cluster_column?: string;
  num_clusters?: number;
  samples_per_cluster?: number;
  sample_percentage?: number;
}

interface SamplingRound {
  round_number: number;
  method: 'random' | 'stratified' | 'systematic' | 'cluster';
  parameters: SamplingParameters;
  output_name?: string;
}

interface SamplingRequest {
  source_ref: string;
  table_key: string;
  create_output_commit: boolean;
  commit_message?: string;
  rounds: SamplingRound[];
  export_residual: boolean;
  residual_output_name?: string;
}

// Validation functions
const validateRandomParams = (params: SamplingParameters): string | null => {
  if (!params.sample_size) {
    return "Sample size is required for random sampling";
  }
  if (params.sample_size <= 0) {
    return "Sample size must be a positive number";
  }
  return null;
};

const validateStratifiedParams = (params: SamplingParameters): string | null => {
  if (!params.sample_size) {
    return "Sample size is required for stratified sampling";
  }
  if (!params.strata_columns || params.strata_columns.length === 0) {
    return "At least one stratification column is required";
  }
  return null;
};

const validateSystematicParams = (params: SamplingParameters): string | null => {
  if (!params.interval) {
    return "Interval is required for systematic sampling";
  }
  if (params.interval <= 0) {
    return "Interval must be a positive number";
  }
  return null;
};

const validateClusterParams = (params: SamplingParameters): string | null => {
  if (!params.cluster_column) {
    return "Cluster column is required";
  }
  if (!params.num_clusters) {
    return "Number of clusters is required";
  }
  if (params.num_clusters <= 0) {
    return "Number of clusters must be positive";
  }
  return null;
};

// Component
const SamplingForm: React.FC<{ datasetId: number }> = ({ datasetId }) => {
  const [method, setMethod] = useState<'random' | 'stratified' | 'systematic' | 'cluster'>('random');
  const [parameters, setParameters] = useState<SamplingParameters>({});
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  // Get default parameters for method
  const getDefaultParams = (method: string): SamplingParameters => {
    switch (method) {
      case 'random':
        return { sample_size: 1000 };
      case 'stratified':
        return { sample_size: 1000, strata_columns: [], proportional: true };
      case 'systematic':
        return { interval: 10, start: 1 };
      case 'cluster':
        return { num_clusters: 10 };
      default:
        return {};
    }
  };

  // Handle method change
  const handleMethodChange = (newMethod: typeof method) => {
    setMethod(newMethod);
    setParameters(getDefaultParams(newMethod));
    setError(null);
  };

  // Validate parameters based on method
  const validateParameters = (): string | null => {
    switch (method) {
      case 'random':
        return validateRandomParams(parameters);
      case 'stratified':
        return validateStratifiedParams(parameters);
      case 'systematic':
        return validateSystematicParams(parameters);
      case 'cluster':
        return validateClusterParams(parameters);
      default:
        return null;
    }
  };

  // Submit sampling job
  const handleSubmit = async () => {
    // Validate
    const validationError = validateParameters();
    if (validationError) {
      setError(validationError);
      return;
    }

    setLoading(true);
    setError(null);

    // Build request
    const request: SamplingRequest = {
      source_ref: "main",
      table_key: "primary",
      create_output_commit: true,
      commit_message: `${method} sampling with ${parameters.sample_size || 'N/A'} samples`,
      rounds: [
        {
          round_number: 1,
          method: method,
          parameters: parameters,
          output_name: `${method} sample`
        }
      ],
      export_residual: false
    };

    try {
      const response = await fetch(`/api/sampling/datasets/${datasetId}/jobs`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('authToken')}`
        },
        body: JSON.stringify(request)
      });

      if (!response.ok) {
        if (response.status === 422) {
          const errorData = await response.json();
          setError(errorData.detail?.[0]?.msg || 'Validation error');
        } else {
          setError(`Request failed: ${response.statusText}`);
        }
        return;
      }

      const result = await response.json();
      console.log('Sampling job created:', result.job_id);
      // Handle success (e.g., redirect to job status page)
      
    } catch (err) {
      setError(`Network error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="sampling-form">
      <h2>Create Sampling Job</h2>
      
      {/* Method Selection */}
      <div className="form-group">
        <label>Sampling Method</label>
        <select 
          value={method} 
          onChange={(e) => handleMethodChange(e.target.value as typeof method)}
        >
          <option value="random">Random Sampling</option>
          <option value="stratified">Stratified Sampling</option>
          <option value="systematic">Systematic Sampling</option>
          <option value="cluster">Cluster Sampling</option>
        </select>
      </div>

      {/* Dynamic Parameter Fields */}
      {method === 'random' && (
        <>
          <div className="form-group">
            <label>Sample Size *</label>
            <input
              type="number"
              min="1"
              value={parameters.sample_size || ''}
              onChange={(e) => setParameters({
                ...parameters,
                sample_size: parseInt(e.target.value) || undefined
              })}
              placeholder="e.g., 1000"
            />
          </div>
          <div className="form-group">
            <label>Random Seed (optional)</label>
            <input
              type="number"
              value={parameters.seed || ''}
              onChange={(e) => setParameters({
                ...parameters,
                seed: parseInt(e.target.value) || undefined
              })}
              placeholder="e.g., 42"
            />
          </div>
        </>
      )}

      {method === 'stratified' && (
        <>
          <div className="form-group">
            <label>Sample Size *</label>
            <input
              type="number"
              min="1"
              value={parameters.sample_size || ''}
              onChange={(e) => setParameters({
                ...parameters,
                sample_size: parseInt(e.target.value) || undefined
              })}
            />
          </div>
          <div className="form-group">
            <label>Stratification Columns *</label>
            <input
              type="text"
              value={parameters.strata_columns?.join(', ') || ''}
              onChange={(e) => setParameters({
                ...parameters,
                strata_columns: e.target.value.split(',').map(s => s.trim()).filter(s => s)
              })}
              placeholder="e.g., region, category"
            />
          </div>
          <div className="form-group">
            <label>
              <input
                type="checkbox"
                checked={parameters.proportional !== false}
                onChange={(e) => setParameters({
                  ...parameters,
                  proportional: e.target.checked
                })}
              />
              Proportional allocation
            </label>
          </div>
        </>
      )}

      {method === 'systematic' && (
        <>
          <div className="form-group">
            <label>Interval *</label>
            <input
              type="number"
              min="1"
              value={parameters.interval || ''}
              onChange={(e) => setParameters({
                ...parameters,
                interval: parseInt(e.target.value) || undefined
              })}
              placeholder="e.g., 10 (every 10th row)"
            />
          </div>
          <div className="form-group">
            <label>Starting Position</label>
            <input
              type="number"
              min="1"
              value={parameters.start || ''}
              onChange={(e) => setParameters({
                ...parameters,
                start: parseInt(e.target.value) || undefined
              })}
              placeholder="Default: 1"
            />
          </div>
        </>
      )}

      {method === 'cluster' && (
        <>
          <div className="form-group">
            <label>Cluster Column *</label>
            <input
              type="text"
              value={parameters.cluster_column || ''}
              onChange={(e) => setParameters({
                ...parameters,
                cluster_column: e.target.value
              })}
              placeholder="e.g., store_id"
            />
          </div>
          <div className="form-group">
            <label>Number of Clusters *</label>
            <input
              type="number"
              min="1"
              value={parameters.num_clusters || ''}
              onChange={(e) => setParameters({
                ...parameters,
                num_clusters: parseInt(e.target.value) || undefined
              })}
              placeholder="e.g., 20"
            />
          </div>
          <div className="form-group">
            <label>Samples per Cluster (optional)</label>
            <input
              type="number"
              min="1"
              value={parameters.samples_per_cluster || ''}
              onChange={(e) => setParameters({
                ...parameters,
                samples_per_cluster: parseInt(e.target.value) || undefined
              })}
              placeholder="Leave empty for all rows"
            />
          </div>
        </>
      )}

      {/* Error Display */}
      {error && (
        <div className="error-message">
          {error}
        </div>
      )}

      {/* Submit Button */}
      <button 
        onClick={handleSubmit} 
        disabled={loading}
        className="submit-button"
      >
        {loading ? 'Creating Job...' : 'Create Sampling Job'}
      </button>

      {/* Parameter Preview (for debugging) */}
      <details>
        <summary>Request Preview</summary>
        <pre>
          {JSON.stringify({
            method,
            parameters
          }, null, 2)}
        </pre>
      </details>
    </div>
  );
};

export default SamplingForm;