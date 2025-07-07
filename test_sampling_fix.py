#!/usr/bin/env python3
"""
Test script to verify the sampling SQL fix
"""
import asyncio
import asyncpg

async def test_sampling_queries():
    # Update with your database connection
    DB_URL = "postgresql://username:password@localhost/database_name"
    
    conn = await asyncpg.connect(DB_URL)
    
    try:
        # Test parameters
        commit_id = "20250707002014_6b5b4118"  # From the failed job
        table_key = "primary"
        sample_size = 10
        
        print("Testing fixed random unseeded query...")
        
        # The fixed query
        query = """
            WITH source_data AS (
                SELECT m.logical_row_id, m.row_hash, r.data as row_data_json
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
                ORDER BY RANDOM()
            )
            SELECT * FROM source_data LIMIT $3
        """
        
        try:
            result = await conn.fetch(query, commit_id.strip(), table_key, sample_size)
            print(f"✓ Query succeeded! Got {len(result)} rows")
            
            if result:
                print("\nSample row:")
                print(f"  logical_row_id: {result[0]['logical_row_id']}")
                print(f"  row_hash: {result[0]['row_hash'][:16]}...")
                
        except Exception as e:
            print(f"✗ Query failed: {e}")
            
        # Also test the seeded version
        print("\n\nTesting random seeded query...")
        
        seeded_query = """
            WITH source_data AS (
                SELECT m.logical_row_id, m.row_hash, r.data as row_data_json,
                       md5(logical_row_id || $4::text) as seeded_random
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
            )
            SELECT logical_row_id, row_hash, row_data_json
            FROM source_data
            ORDER BY seeded_random
            LIMIT $3
        """
        
        try:
            result = await conn.fetch(seeded_query, commit_id.strip(), table_key, sample_size, "42")
            print(f"✓ Seeded query succeeded! Got {len(result)} rows")
        except Exception as e:
            print(f"✗ Seeded query failed: {e}")
            
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(test_sampling_queries())