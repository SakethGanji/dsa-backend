#!/usr/bin/env python3
"""
Reset database by dropping all objects and recreating from schema.sql
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import os

# Database connection parameters
DB_PARAMS = {
    'host': 'localhost',
    'user': 'postgres',
    'password': 'postgres',
    'database': 'postgres'
}

def reset_database():
    """Drop all database objects and recreate from schema.sql"""
    
    conn = None
    cursor = None
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_PARAMS)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        print("Connected to database")
        
        # Drop all schemas except system schemas
        print("Dropping all schemas...")
        cursor.execute("""
            DO $$ 
            DECLARE
                r RECORD;
            BEGIN
                -- Drop all schemas except system schemas
                FOR r IN (SELECT nspname FROM pg_namespace 
                         WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                         AND nspname NOT LIKE 'pg_temp_%'
                         AND nspname NOT LIKE 'pg_toast_temp_%')
                LOOP
                    EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(r.nspname) || ' CASCADE';
                    RAISE NOTICE 'Dropped schema: %', r.nspname;
                END LOOP;
            END $$;
        """)
        
        # Drop all extensions
        print("Dropping all extensions...")
        cursor.execute("""
            DO $$ 
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT extname FROM pg_extension WHERE extname != 'plpgsql')
                LOOP
                    EXECUTE 'DROP EXTENSION IF EXISTS ' || quote_ident(r.extname) || ' CASCADE';
                    RAISE NOTICE 'Dropped extension: %', r.extname;
                END LOOP;
            END $$;
        """)
        
        # Recreate public schema
        print("Recreating public schema...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS public;")
        cursor.execute("GRANT ALL ON SCHEMA public TO postgres;")
        cursor.execute("GRANT ALL ON SCHEMA public TO public;")
        
        # Drop all remaining tables in public schema (if any)
        print("Dropping any remaining tables...")
        cursor.execute("""
            DO $$ 
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
                LOOP
                    EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
                    RAISE NOTICE 'Dropped table: %', r.tablename;
                END LOOP;
            END $$;
        """)
        
        # Drop all sequences
        print("Dropping all sequences...")
        cursor.execute("""
            DO $$ 
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public')
                LOOP
                    EXECUTE 'DROP SEQUENCE IF EXISTS public.' || quote_ident(r.sequence_name) || ' CASCADE';
                    RAISE NOTICE 'Dropped sequence: %', r.sequence_name;
                END LOOP;
            END $$;
        """)
        
        # Drop all functions
        print("Dropping all functions...")
        cursor.execute("""
            DO $$ 
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT proname, oidvectortypes(proargtypes) as args 
                         FROM pg_proc 
                         WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public'))
                LOOP
                    EXECUTE 'DROP FUNCTION IF EXISTS public.' || quote_ident(r.proname) || '(' || r.args || ') CASCADE';
                    RAISE NOTICE 'Dropped function: %', r.proname;
                END LOOP;
            END $$;
        """)
        
        # Drop all types
        print("Dropping all types...")
        cursor.execute("""
            DO $$ 
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT typname FROM pg_type 
                         WHERE typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                         AND typtype = 'e')
                LOOP
                    EXECUTE 'DROP TYPE IF EXISTS public.' || quote_ident(r.typname) || ' CASCADE';
                    RAISE NOTICE 'Dropped type: %', r.typname;
                END LOOP;
            END $$;
        """)
        
        print("\nAll database objects dropped successfully!")
        
        # Now recreate from schema.sql
        print("\nRecreating database from schema.sql...")
        
        # Check if schema.sql exists
        schema_file = 'schema.sql'
        if not os.path.exists(schema_file):
            print(f"ERROR: {schema_file} not found!")
            return False
            
        # Read and execute schema.sql
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
            
        # Execute the schema
        cursor.execute(schema_sql)
        
        print("Database recreated successfully from schema.sql!")
        
        # Refresh materialized view
        print("\nRefreshing materialized view...")
        cursor.execute("REFRESH MATERIALIZED VIEW dsa_search.datasets_summary;")
        print("Materialized view refreshed successfully!")
        
        # Insert default roles
        print("\nInserting default roles...")
        cursor.execute("""
            INSERT INTO dsa_auth.roles (role_name, description) VALUES
                ('admin', 'Full system administrator'),
                ('analyst', 'Data analyst with read/write permissions'),
                ('viewer', 'Read-only access')
            ON CONFLICT (role_name) DO NOTHING;
        """)
        print("Default roles inserted successfully!")
        
        # Show created objects
        print("\nCreated objects:")
        
        # List schemas
        cursor.execute("""
            SELECT nspname FROM pg_namespace 
            WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            AND nspname NOT LIKE 'pg_temp_%'
            AND nspname NOT LIKE 'pg_toast_temp_%'
            ORDER BY nspname;
        """)
        schemas = cursor.fetchall()
        if schemas:
            print("\nSchemas:")
            for schema in schemas:
                print(f"  - {schema[0]}")
        
        # List tables
        cursor.execute("""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, tablename;
        """)
        tables = cursor.fetchall()
        if tables:
            print("\nTables:")
            for schema, table in tables:
                print(f"  - {schema}.{table}")
                
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    print("=== Database Reset Tool ===")
    print("This will DROP ALL database objects and recreate from schema.sql")
    print("WARNING: This will delete ALL data including sequences/IDs!")
    
    response = input("\nAre you sure you want to continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Aborted.")
        sys.exit(0)
    
    if reset_database():
        print("\nDatabase reset completed successfully!")
    else:
        print("\nDatabase reset failed!")
        sys.exit(1)