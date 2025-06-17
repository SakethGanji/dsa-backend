import asyncio
import os
from typing import AsyncGenerator, Optional

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from ..infrastructure.database import DatabaseManager


class TestDatabase:
    """Test database management."""
    
    def __init__(self):
        # Use test database URL
        self.database_url = os.getenv("TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/test_db")
        self.engine = create_async_engine(
            self.database_url,
            echo=False,
            poolclass=NullPool,  # Disable pooling for tests
        )
        self.async_session_factory = sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    
    async def create_tables(self):
        """Create all tables for testing."""
        # This would normally use SQLAlchemy models
        # For now, we'll use raw SQL
        async with self.engine.connect() as conn:
            # Create tables as needed
            pass
    
    async def drop_tables(self):
        """Drop all tables after testing."""
        async with self.engine.connect() as conn:
            # Drop tables as needed
            pass
    
    async def get_session(self) -> AsyncSession:
        """Get a test session."""
        async with self.async_session_factory() as session:
            yield session
    
    async def cleanup(self):
        """Clean up database connections."""
        await self.engine.dispose()


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def test_db():
    """Create test database instance."""
    db = TestDatabase()
    await db.create_tables()
    yield db
    await db.drop_tables()
    await db.cleanup()


@pytest.fixture
async def db_session(test_db: TestDatabase) -> AsyncGenerator[AsyncSession, None]:
    """Get a database session for testing."""
    async with test_db.async_session_factory() as session:
        async with session.begin():
            yield session
            await session.rollback()


@pytest.fixture
async def db_manager(test_db: TestDatabase) -> DatabaseManager:
    """Get a test database manager."""
    manager = DatabaseManager(test_db.database_url)
    yield manager
    await manager.close()


class BaseRepositoryTest:
    """Base class for repository tests."""
    
    @pytest.fixture(autouse=True)
    async def setup(self, db_session: AsyncSession):
        """Set up test data."""
        self.session = db_session
        await self.create_test_data()
    
    async def create_test_data(self):
        """Override to create test data."""
        pass


class BaseServiceTest:
    """Base class for service tests."""
    
    @pytest.fixture(autouse=True)
    async def setup(self, db_manager: DatabaseManager):
        """Set up test dependencies."""
        self.db_manager = db_manager
        await self.create_test_dependencies()
    
    async def create_test_dependencies(self):
        """Override to create test dependencies."""
        pass