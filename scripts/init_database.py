#!/usr/bin/env python3
"""Initialize the database with the required schema."""

import asyncio
import asyncpg
import os
import sys
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import get_settings


async def init_database():
    """Initialize the database with the schema."""
    settings = get_settings()
    
    print(f"Connecting to database: {settings.database_url}")
    
    # Connect to the database
    conn = await asyncpg.connect(settings.database_url)
    
    try:
        # Read the fixed schema file
        schema_path = Path(__file__).parent.parent / "src" / "sql" / "schema_fixed.sql"
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        
        print("Executing schema...")
        
        # Execute the schema
        await conn.execute(schema_sql)
        
        print("Database schema initialized successfully!")
        
        # Verify the schemas were created
        schemas = await conn.fetch("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('dsa_auth', 'dsa_core', 'dsa_jobs')
            ORDER BY schema_name
        """)
        
        print("\nCreated schemas:")
        for schema in schemas:
            print(f"  - {schema['schema_name']}")
        
        # Count tables in each schema
        table_counts = await conn.fetch("""
            SELECT 
                table_schema,
                COUNT(*) as table_count
            FROM information_schema.tables
            WHERE table_schema IN ('dsa_auth', 'dsa_core', 'dsa_jobs')
            AND table_type = 'BASE TABLE'
            GROUP BY table_schema
            ORDER BY table_schema
        """)
        
        print("\nTable counts per schema:")
        for count in table_counts:
            print(f"  - {count['table_schema']}: {count['table_count']} tables")
        
        # Verify roles were created
        roles = await conn.fetch("""
            SELECT role_name, description 
            FROM dsa_auth.roles 
            ORDER BY id
        """)
        
        print("\nCreated roles:")
        for role in roles:
            print(f"  - {role['role_name']}: {role['description']}")
            
    except Exception as e:
        print(f"Error initializing database: {e}")
        raise
    finally:
        await conn.close()


async def reset_database():
    """Reset the database by dropping and recreating schemas."""
    settings = get_settings()
    
    print(f"Connecting to database for reset: {settings.database_url}")
    
    conn = await asyncpg.connect(settings.database_url)
    
    try:
        # Drop schemas in reverse order
        print("Dropping existing schemas...")
        await conn.execute("DROP SCHEMA IF EXISTS dsa_jobs CASCADE")
        await conn.execute("DROP SCHEMA IF EXISTS dsa_core CASCADE")
        await conn.execute("DROP SCHEMA IF EXISTS dsa_auth CASCADE")
        
        print("Schemas dropped. Reinitializing...")
        
    except Exception as e:
        print(f"Error resetting database: {e}")
        raise
    finally:
        await conn.close()
    
    # Now initialize fresh
    await init_database()


async def create_test_user():
    """Create a test user for development."""
    settings = get_settings()
    conn = await asyncpg.connect(settings.database_url)
    
    try:
        from passlib.context import CryptContext
        pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        
        # Create test user
        test_password = "testpass123"
        password_hash = pwd_context.hash(test_password)
        
        user = await conn.fetchrow("""
            INSERT INTO dsa_auth.users (soeid, password_hash, role_id)
            SELECT 'TEST999', $1, id FROM dsa_auth.roles WHERE role_name = 'admin'
            ON CONFLICT (soeid) DO UPDATE 
            SET password_hash = EXCLUDED.password_hash
            RETURNING id, soeid
        """, password_hash)
        
        print(f"\nCreated test user:")
        print(f"  - SOEID: {user['soeid']}")
        print(f"  - Password: {test_password}")
        print(f"  - Role: admin")
        
    except Exception as e:
        print(f"Error creating test user: {e}")
    finally:
        await conn.close()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Initialize DSA database")
    parser.add_argument("--reset", action="store_true", help="Drop and recreate all schemas")
    parser.add_argument("--test-user", action="store_true", help="Create a test user")
    
    args = parser.parse_args()
    
    if args.reset:
        asyncio.run(reset_database())
    else:
        asyncio.run(init_database())
    
    if args.test_user:
        asyncio.run(create_test_user())


if __name__ == "__main__":
    main()