-- Debug query to check why job endpoint is failing
-- Run this query in your PostgreSQL database to investigate

-- 1. Check if the job exists
SELECT 
    ar.id,
    ar.run_type,
    ar.status,
    ar.dataset_id,
    ar.created_at,
    ar.error_message,
    length(ar.run_parameters::text) as params_length,
    length(ar.output_summary::text) as output_length
FROM dsa_jobs.analysis_runs ar
WHERE ar.id = '917f045a-ec63-47a1-b192-35149508f452';

-- 2. Check for any jobs with similar IDs (in case of UUID formatting issues)
SELECT 
    ar.id::text,
    ar.status,
    ar.created_at
FROM dsa_jobs.analysis_runs ar
WHERE ar.id::text LIKE '%917f045a%'
ORDER BY ar.created_at DESC
LIMIT 10;

-- 3. Check the most recent jobs to see if this one is there
SELECT 
    ar.id::text as job_id,
    ar.run_type,
    ar.status,
    ar.created_at,
    ar.error_message
FROM dsa_jobs.analysis_runs ar
ORDER BY ar.created_at DESC
LIMIT 20;

-- 4. Check if there are any issues with the joins
SELECT 
    ar.id,
    ar.dataset_id,
    d.id as dataset_id_from_join,
    d.name as dataset_name,
    ar.user_id,
    u.id as user_id_from_join,
    u.soeid as user_soeid
FROM dsa_jobs.analysis_runs ar
LEFT JOIN dsa_core.datasets d ON ar.dataset_id = d.id
LEFT JOIN dsa_auth.users u ON ar.user_id = u.id
WHERE ar.id = '917f045a-ec63-47a1-b192-35149508f452';