#!/usr/bin/env python3
"""
Apply import performance optimizations to the DSA platform.

This script:
1. Applies database indexes for import performance
2. Updates the import handler to use streaming
3. Updates the import executor to use batch processing
"""

import asyncio
import asyncpg
import os
import shutil
from pathlib import Path


async def apply_database_indexes(conn: asyncpg.Connection):
    """Apply performance indexes to the database."""
    print("Applying database indexes...")
    
    indexes_sql = Path(__file__).parent.parent / "sql" / "import_performance_indexes.sql"
    
    with open(indexes_sql, 'r') as f:
        sql = f.read()
    
    # Execute the SQL script
    await conn.execute(sql)
    
    print("✓ Database indexes applied successfully")


async def backup_and_replace_files():
    """Backup original files and replace with optimized versions."""
    src_dir = Path(__file__).parent.parent
    
    # Files to replace
    replacements = [
        {
            "original": src_dir / "features" / "versioning" / "queue_import_job.py",
            "optimized": src_dir / "features" / "versioning" / "queue_import_job_optimized.py",
            "backup": src_dir / "features" / "versioning" / "queue_import_job.py.backup"
        },
        {
            "original": src_dir / "workers" / "import_executor.py",
            "optimized": src_dir / "workers" / "import_executor_optimized.py",
            "backup": src_dir / "workers" / "import_executor.py.backup"
        }
    ]
    
    for replacement in replacements:
        original = replacement["original"]
        optimized = replacement["optimized"]
        backup = replacement["backup"]
        
        if not optimized.exists():
            print(f"⚠️  Optimized file not found: {optimized}")
            continue
        
        # Create backup
        if original.exists():
            print(f"Backing up {original.name} -> {backup.name}")
            shutil.copy2(original, backup)
        
        # Replace with optimized version
        print(f"Installing optimized {original.name}")
        shutil.copy2(optimized, original)
    
    print("✓ File replacements completed")


async def main():
    """Main migration function."""
    print("=" * 60)
    print("DSA Import Performance Optimization Migration")
    print("=" * 60)
    
    # Get database connection
    database_url = os.getenv(
        "DATABASE_URL", 
        "postgresql://postgres:postgres@localhost:5432/postgres"
    )
    
    print("\nConnecting to database...")
    conn = await asyncpg.connect(database_url)
    
    try:
        # Apply database changes
        await apply_database_indexes(conn)
        
        # Replace files with optimized versions
        await backup_and_replace_files()
        
        print("\n✅ Migration completed successfully!")
        print("\nNext steps:")
        print("1. Restart your application to use the optimized import handlers")
        print("2. Monitor import performance with the new implementation")
        print("3. Original files have been backed up with .backup extension")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        raise
    finally:
        await conn.close()


def rollback():
    """Rollback the migration by restoring backup files."""
    print("Rolling back import optimizations...")
    
    src_dir = Path(__file__).parent.parent
    
    # Files to restore
    files_to_restore = [
        {
            "backup": src_dir / "features" / "versioning" / "queue_import_job.py.backup",
            "original": src_dir / "features" / "versioning" / "queue_import_job.py"
        },
        {
            "backup": src_dir / "workers" / "import_executor.py.backup",
            "original": src_dir / "workers" / "import_executor.py"
        }
    ]
    
    for file_pair in files_to_restore:
        backup = file_pair["backup"]
        original = file_pair["original"]
        
        if backup.exists():
            print(f"Restoring {original.name} from backup")
            shutil.copy2(backup, original)
        else:
            print(f"⚠️  Backup not found: {backup}")
    
    print("✓ Rollback completed")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "rollback":
        rollback()
    else:
        asyncio.run(main())