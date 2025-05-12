import asyncio
import os
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import text
from app.db.connection import get_engine
import bcrypt

SCHEMA_REL_PATH = "../../sql/schema.sql"

# Optional seed data
raw = b"ChangeMe123!"
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
        soeid="admin001",
        password_hash=hashed
    )
]

async def init_db():
    engine: AsyncEngine = await get_engine()
    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_REL_PATH)

    # Read and split schema into individual statements
    with open(schema_path, "r") as f:
        raw_sql = f.read()
    statements = [stmt.strip() for stmt in raw_sql.split(";") if stmt.strip()]

    async with engine.begin() as conn:
        # Create schema first if using schema
        # await conn.execute(CREATE_SCHEMA)
        # print("Core schema created.")

        for stmt in statements:
            if stmt:  # Skip empty statements
                await conn.execute(text(stmt))

        print("Schema created.")

        for seed_stmt in SEED_SQL:
            await conn.execute(seed_stmt)

        print("Seed data inserted (if missing).")


if __name__ == "__main__":
    asyncio.run(init_db())