import asyncio
import os
import sys
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import text
import bcrypt

# Add parent directory to path for direct script execution
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.db.connection import get_engine

SCHEMA_REL_PATH = "../../sql/schema.sql"

# Optional seed data
raw = b"string"
hashed = bcrypt.hashpw(raw, bcrypt.gensalt()).decode()  # eg "$2b$12$..."

# Create core schema first
CREATE_SCHEMA = text("CREATE SCHEMA IF NOT EXISTS core;")

# Updated seed SQL to match the new schema structure
SEED_SQL = [
    text("""
        INSERT INTO roles (role_name, description)
        VALUES ('admin', 'Administrator'), ('analyst', 'Data Analyst')
        ON CONFLICT (role_name) DO NOTHING;
    """),
    text("""
        INSERT INTO users (soeid, password_hash, role_id)
        SELECT :soeid, :password_hash, id
        FROM roles WHERE role_name = 'admin'
        ON CONFLICT (soeid) DO NOTHING;
    """).bindparams(
        soeid="bg54677",
        password_hash=hashed
    )
]

async def init_db():
    engine: AsyncEngine = await get_engine()
    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_REL_PATH)

    # Read and split schema into individual statements
    with open(schema_path, "r") as f:
        raw_sql = f.read()
    
    # Better SQL statement splitting to handle DO blocks
    statements = []
    current_stmt = []
    in_dollar_quote = False
    
    for line in raw_sql.split('\n'):
        current_stmt.append(line)
        
        # Check for DO $$ blocks
        if '$$' in line:
            in_dollar_quote = not in_dollar_quote
        
        # Only split on semicolon if not inside a DO block
        if ';' in line and not in_dollar_quote:
            statements.append('\n'.join(current_stmt))
            current_stmt = []
    
    # Add any remaining statement
    if current_stmt:
        statements.append('\n'.join(current_stmt))

    async with engine.begin() as conn:
        # Create schema first if using schema
        # await conn.execute(CREATE_SCHEMA)
        # print("Core schema created.")

        for stmt in statements:
            stmt = stmt.strip()
            if stmt and not stmt.startswith('--'):  # Skip empty statements and comments
                await conn.execute(text(stmt))

        print("Schema created.")

        for seed_stmt in SEED_SQL:
            await conn.execute(seed_stmt)

        print("Seed data inserted (if missing).")


if __name__ == "__main__":
    asyncio.run(init_db())