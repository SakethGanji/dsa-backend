import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

load_dotenv()


class DatabaseManager:
    """Manages database connections and transactions."""
    
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is not set")
        
        # Create engine with connection pooling
        self.engine: AsyncEngine = create_async_engine(
            self.database_url,
            echo=False,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
        )
        
        # Create session factory
        self.async_session_factory = sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    
    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get a database session."""
        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
    
    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[AsyncSession, None]:
        """Create a new transaction."""
        async with self.get_session() as session:
            async with session.begin():
                yield session
    
    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[AsyncConnection, None]:
        """Get a raw database connection."""
        async with self.engine.connect() as connection:
            yield connection
    
    async def close(self):
        """Close all database connections."""
        await self.engine.dispose()
    
    async def execute_raw(self, query: str, params: Optional[dict] = None):
        """Execute a raw SQL query."""
        async with self.get_connection() as connection:
            result = await connection.execute(query, params or {})
            return result
    
    async def health_check(self) -> bool:
        """Check if database is accessible."""
        try:
            async with self.get_connection() as connection:
                await connection.execute("SELECT 1")
            return True
        except Exception:
            return False


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


async def close_database():
    """Close the global database connection."""
    global _db_manager
    if _db_manager is not None:
        await _db_manager.close()
        _db_manager = None