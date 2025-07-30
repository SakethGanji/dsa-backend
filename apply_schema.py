#!/usr/bin/env python3
"""
Apply schema.sql to a fresh PostgreSQL database.
"""

import psycopg2
import sys
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def apply_schema(
    dbname="dsa",
    user="postgres", 
    password=None,
    host="localhost",
    port=5432,
    schema_file="schema.sql"
):
    """Apply schema.sql to the specified database."""
    
    # Check if schema file exists
    if not os.path.exists(schema_file):
        print(f"Error: Schema file '{schema_file}' not found!")
        return False
    
    # Read schema file
    print(f"Reading schema from {schema_file}...")
    with open(schema_file, 'r') as f:
        schema_sql = f.read()
    
    # Connect to database
    try:
        print(f"Connecting to database '{dbname}' on {host}:{port}...")
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        print("Applying schema...")
        cur.execute(schema_sql)
        
        print("Schema applied successfully!")
        
        # Show summary of created objects
        print("\nCreated schemas:")
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name LIKE 'dsa_%'
            ORDER BY schema_name;
        """)
        for row in cur.fetchall():
            print(f"  - {row[0]}")
        
        print("\nCreated tables:")
        cur.execute("""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE schemaname LIKE 'dsa_%'
            ORDER BY schemaname, tablename;
        """)
        for row in cur.fetchall():
            print(f"  - {row[0]}.{row[1]}")
        
        cur.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def main():
    """Main function to handle command line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Apply schema.sql to a PostgreSQL database')
    parser.add_argument('--dbname', '-d', default='dsa', help='Database name (default: dsa)')
    parser.add_argument('--user', '-U', default='postgres', help='Database user (default: postgres)')
    parser.add_argument('--password', '-W', help='Database password (prompt if not provided)')
    parser.add_argument('--host', '-h', default='localhost', help='Database host (default: localhost)')
    parser.add_argument('--port', '-p', type=int, default=5432, help='Database port (default: 5432)')
    parser.add_argument('--schema-file', '-f', default='schema.sql', help='Schema file path (default: schema.sql)')
    
    args = parser.parse_args()
    
    # Get password from environment if not provided
    password = args.password
    if password is None:
        password = os.environ.get('PGPASSWORD')
    
    # If still no password, prompt for it
    if password is None:
        import getpass
        password = getpass.getpass(f"Password for user {args.user}: ")
    
    # Apply schema
    success = apply_schema(
        dbname=args.dbname,
        user=args.user,
        password=password,
        host=args.host,
        port=args.port,
        schema_file=args.schema_file
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()