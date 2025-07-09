"""Migration to drop the deprecated commit_statistics table."""

import asyncio
import asyncpg
from src.core.config import get_settings

async def run_migration():
    """Drop the deprecated commit_statistics table."""
    settings = get_settings()
    
    # Connect to database
    conn = await asyncpg.connect(settings.database_url)
    
    try:
        # Drop the table
        await conn.execute("""
            DROP TABLE IF EXISTS dsa_core.commit_statistics CASCADE;
        """)
        
        print("Successfully dropped commit_statistics table")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(run_migration())