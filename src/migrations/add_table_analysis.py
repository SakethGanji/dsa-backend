"""Migration to add table_analysis table for storing comprehensive table statistics."""

import asyncio
import asyncpg
from src.core.config import get_settings

async def run_migration():
    """Add table_analysis table to store pre-computed analysis."""
    settings = get_settings()
    
    # Connect to database
    conn = await asyncpg.connect(settings.database_url)
    
    try:
        # Create table_analysis table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dsa_core.table_analysis (
                commit_id TEXT NOT NULL,
                table_key TEXT NOT NULL,
                analysis JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (commit_id, table_key),
                FOREIGN KEY (commit_id) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE
            )
        """)
        
        # Create indexes for performance
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_table_analysis_commit_id 
            ON dsa_core.table_analysis(commit_id)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_table_analysis_table_key 
            ON dsa_core.table_analysis(table_key)
        """)
        
        print("Successfully created table_analysis table and indexes")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(run_migration())