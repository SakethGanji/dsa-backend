#!/usr/bin/env python3
"""Script to fix file paths in the database to include proper URI schemes"""

import asyncio
import os
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

# Get database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres")


async def fix_file_paths():
    """Add file:// prefix to paths that don't have a URI scheme"""
    engine = create_async_engine(DATABASE_URL)
    
    async with engine.begin() as conn:
        # First, let's see what paths we have
        result = await conn.execute(text("""
            SELECT id, file_path 
            FROM files 
            WHERE file_path NOT LIKE '%://%'
            AND file_path LIKE '/data/artifacts/%'
        """))
        
        rows = result.fetchall()
        print(f"Found {len(rows)} file paths without URI scheme")
        
        for row in rows:
            file_id, file_path = row
            new_path = f"file://{file_path}"
            print(f"Updating file {file_id}: {file_path} -> {new_path}")
            
            await conn.execute(text("""
                UPDATE files 
                SET file_path = :new_path 
                WHERE id = :file_id
            """), {"new_path": new_path, "file_id": file_id})
        
        print(f"Updated {len(rows)} file paths")
    
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(fix_file_paths())