#!/usr/bin/env python3
"""
Test script to diagnose job endpoint failures
"""
import asyncio
import asyncpg
import json
from uuid import UUID

async def test_job_retrieval():
    # Database connection parameters - update these
    DB_URL = "postgresql://username:password@localhost/database_name"
    
    job_id = UUID("917f045a-ec63-47a1-b192-35149508f452")
    
    conn = await asyncpg.connect(DB_URL)
    
    try:
        # Test 1: Check if job exists
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM dsa_jobs.analysis_runs WHERE id = $1)",
            job_id
        )
        print(f"Job exists: {exists}")
        
        if not exists:
            # Check recent jobs
            recent_jobs = await conn.fetch(
                """
                SELECT id::text, status, created_at 
                FROM dsa_jobs.analysis_runs 
                ORDER BY created_at DESC 
                LIMIT 10
                """
            )
            print("\nRecent jobs:")
            for job in recent_jobs:
                print(f"  {job['id']}: {job['status']} at {job['created_at']}")
            return
        
        # Test 2: Get basic job info
        basic_info = await conn.fetchrow(
            """
            SELECT id, run_type, status, dataset_id, user_id,
                   length(run_parameters::text) as param_len,
                   length(output_summary::text) as output_len
            FROM dsa_jobs.analysis_runs 
            WHERE id = $1
            """,
            job_id
        )
        print(f"\nBasic job info:")
        print(f"  Type: {basic_info['run_type']}")
        print(f"  Status: {basic_info['status']}")
        print(f"  Dataset ID: {basic_info['dataset_id']}")
        print(f"  User ID: {basic_info['user_id']}")
        print(f"  Param length: {basic_info['param_len']}")
        print(f"  Output length: {basic_info['output_len']}")
        
        # Test 3: Check if dataset exists
        dataset_exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM dsa_core.datasets WHERE id = $1)",
            basic_info['dataset_id']
        )
        print(f"\nDataset exists: {dataset_exists}")
        
        # Test 4: Check if user exists
        user_exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM dsa_auth.users WHERE id = $1)",
            basic_info['user_id']
        )
        print(f"User exists: {user_exists}")
        
        # Test 5: Try the full query
        try:
            full_row = await conn.fetchrow(
                """
                SELECT 
                    ar.id,
                    ar.run_type,
                    ar.status,
                    ar.dataset_id,
                    d.name as dataset_name,
                    ar.source_commit_id,
                    ar.user_id,
                    u.soeid as user_soeid,
                    ar.run_parameters,
                    ar.output_summary,
                    ar.error_message,
                    ar.created_at,
                    ar.completed_at
                FROM dsa_jobs.analysis_runs ar
                LEFT JOIN dsa_core.datasets d ON ar.dataset_id = d.id
                LEFT JOIN dsa_auth.users u ON ar.user_id = u.id
                WHERE ar.id = $1
                """,
                job_id
            )
            
            if full_row:
                print("\nFull query succeeded")
                print(f"  Dataset name: {full_row['dataset_name']}")
                print(f"  User SOEID: {full_row['user_soeid']}")
                
                # Check JSON fields
                if full_row['run_parameters']:
                    try:
                        params = json.loads(full_row['run_parameters']) if isinstance(full_row['run_parameters'], str) else full_row['run_parameters']
                        print(f"  Run parameters parsed successfully: {type(params)}")
                    except Exception as e:
                        print(f"  ERROR parsing run_parameters: {e}")
                        
                if full_row['output_summary']:
                    try:
                        output = json.loads(full_row['output_summary']) if isinstance(full_row['output_summary'], str) else full_row['output_summary']
                        print(f"  Output summary parsed successfully: {type(output)}")
                    except Exception as e:
                        print(f"  ERROR parsing output_summary: {e}")
            else:
                print("\nFull query returned no results!")
                
        except Exception as e:
            print(f"\nERROR in full query: {e}")
            
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(test_job_retrieval())