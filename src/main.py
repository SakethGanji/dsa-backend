"""Main FastAPI application."""

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from .core.config import get_settings
from .core.database import DatabasePool
from .core.dependencies import (
    set_database_pool,
    set_parser_factory,
    set_stats_calculator
)
from .core.infrastructure.services import FileParserFactory, DefaultStatisticsCalculator
from .api import users, datasets, versioning

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global database pool
db_pool: DatabasePool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    global db_pool
    
    settings = get_settings()
    logger.info("Starting DSA Platform...")
    
    # Initialize database pool
    db_pool = DatabasePool(settings.database_url)
    await db_pool.initialize()
    logger.info("Database pool initialized")
    
    # Initialize global dependencies
    set_database_pool(db_pool)
    set_parser_factory(FileParserFactory())
    set_stats_calculator(DefaultStatisticsCalculator())
    logger.info("Dependencies initialized")
    
    yield
    
    # Cleanup
    if db_pool:
        await db_pool.close()
    logger.info("Database pool closed")


# Create FastAPI app
app = FastAPI(
    title="DSA Platform API",
    version="2.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency to get database pool
async def get_db_pool() -> DatabasePool:
    """Get the global database pool."""
    if not db_pool:
        raise RuntimeError("Database pool not initialized")
    return db_pool


# Include routers
app.include_router(users.router, prefix="/api")
app.include_router(datasets.router, prefix="/api")
app.include_router(versioning.router, prefix="/api")

# Override dependencies using FastAPI's dependency_overrides
app.dependency_overrides[users.get_db_pool] = get_db_pool
app.dependency_overrides[datasets.get_db_pool] = get_db_pool


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "DSA Platform API v2.0"}


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "version": "2.0.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)