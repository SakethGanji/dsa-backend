"""Main FastAPI application - REFACTORED with new error handling."""

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import asyncio

from .core.config import get_settings
from .core.database import DatabasePool
from .core.dependencies import (
    set_database_pool,
    set_parser_factory,
    set_stats_calculator
)
from .infrastructure.services import FileParserFactory, DefaultStatisticsCalculator

# Import new error handling
from .api.error_handlers import register_error_handlers

# Import API routers
from .api import users, datasets, versioning, jobs, search, sampling, exploration, workbench, downloads

# Import workers
from .workers.job_worker import JobWorker
from .workers.import_executor import ImportJobExecutor
from .workers.sampling_executor import SamplingJobExecutor
from .workers.exploration_executor import ExplorationExecutor
from .workers.sql_transform_executor import SqlTransformExecutor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global database pool
db_pool: DatabasePool = None
worker_task = None
worker = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    global db_pool, worker_task, worker
    
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
    
    # Initialize and start job worker
    executors = {
        'import': ImportJobExecutor(db_pool),
        'sampling': SamplingJobExecutor(db_pool),
        'exploration': ExplorationExecutor(db_pool),
        'sql_transform': SqlTransformExecutor(db_pool)
    }
    
    worker = JobWorker(db_pool, executors)
    worker_task = asyncio.create_task(worker.start())
    logger.info("Job worker started")
    
    yield
    
    # Cleanup
    logger.info("Shutting down DSA Platform...")
    
    # Stop worker
    if worker:
        await worker.stop()
    if worker_task:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
    
    # Close database pool
    if db_pool:
        await db_pool.close()
    
    logger.info("Shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="DSA Platform API",
    description="Data Storage and Analytics Platform",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register error handlers - NEW!
register_error_handlers(app)

# Include routers
app.include_router(users.router, prefix="/api/v1")
app.include_router(datasets.router, prefix="/api/v1")
app.include_router(versioning.router, prefix="/api/v1")
app.include_router(jobs.router, prefix="/api/v1")
app.include_router(search.router, prefix="/api/v1")
app.include_router(sampling.router, prefix="/api/v1")
app.include_router(exploration.router, prefix="/api/v1")
app.include_router(workbench.router, prefix="/api/v1")
app.include_router(downloads.router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "DSA Platform API", "version": "1.0.0"}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "database": "connected" if db_pool and not db_pool._closed else "disconnected",
        "worker": "running" if worker_task and not worker_task.done() else "stopped"
    }


# Request ID middleware for tracing
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Add request ID to all requests for tracing."""
    import uuid
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    request.state.request_id = request_id
    
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# Example of how error handling works:
# 1. Handler raises domain exception: raise EntityNotFoundException("Dataset", 123)
# 2. Error handler converts to HTTP response: {"error": "NOT_FOUND", "message": "Dataset 123 not found"}
# 3. Client receives proper HTTP status (404) with structured error