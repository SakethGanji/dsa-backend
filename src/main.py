"""Main FastAPI application with comprehensive error handling."""

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import asyncio
from urllib.parse import quote_plus, urlparse, urlunparse

from .infrastructure.config import get_settings
from .infrastructure.postgres.database import DatabasePool
from .infrastructure.external.password_manager import get_password_manager
from .api.dependencies import (
    set_database_pool,
    set_parser_factory,
    set_stats_calculator,
    set_event_bus
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

# Import event system
from .core.events import EventHandlerRegistry, InMemoryEventBus
from .infrastructure.postgres.event_store import PostgresEventStore
from .features.search.event_handlers import SearchIndexEventHandler
from .features.common.event_handlers import (
    CacheInvalidationHandler, AuditLogHandler, NotificationHandler
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global references for health check
app_state = {
    "db_pool": None,
    "worker_task": None,
    "event_bus": None,
    "event_registry": None
}

# Global database pool
db_pool: DatabasePool = None
worker_task = None
worker = None
event_bus = None
event_registry = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    global db_pool, worker_task, worker, event_bus, event_registry
    
    settings = get_settings()
    logger.info("Starting DSA Platform...")
    
    # Build DSN from individual PostgreSQL components
    password = settings.POSTGRESQL_PASSWORD
    
    # Override password with NGC secret if configured
    if settings.POSTGRESQL_PASSWORD_SECRET_NAME and settings.POSTGRESQL_PASSWORD_SECRET_NAME.lower() != "none":
        try:
            password_manager = get_password_manager()
            password = password_manager.postgresql_fetch()
            if password:
                logger.info("Using dynamic password from secret manager")
        except Exception as e:
            logger.error(f"Failed to fetch dynamic password: {e}")
            logger.info("Falling back to environment variable password")
            password = settings.POSTGRESQL_PASSWORD
    
    # Build the final DSN with the resolved password
    dsn = f"postgresql://{settings.POSTGRESQL_USER}:{quote_plus(password)}@{settings.POSTGRESQL_HOST}:{settings.POSTGRESQL_PORT}/{settings.POSTGRESQL_DATABASE}"
    
    db_pool = DatabasePool(dsn)
    await db_pool.initialize()
    logger.info("Database pool initialized")
    
    # Store in app_state for health check
    app_state["db_pool"] = db_pool
    
    # Initialize global dependencies
    set_database_pool(db_pool)
    set_parser_factory(FileParserFactory())
    set_stats_calculator(DefaultStatisticsCalculator())
    
    # Initialize event system
    logger.info("Initializing event system...")
    event_store = PostgresEventStore(db_pool)
    event_bus = InMemoryEventBus(store_events=True)
    event_bus.set_event_store(event_store)
    
    # Create and register event handlers
    event_registry = EventHandlerRegistry()
    
    # Register handlers
    event_registry.register_handler(SearchIndexEventHandler(db_pool))
    event_registry.register_handler(AuditLogHandler(db_pool))
    event_registry.register_handler(CacheInvalidationHandler())  # No cache configured yet
    event_registry.register_handler(NotificationHandler())  # No notification service yet
    
    # Wire handlers to event bus
    event_registry.wire_to_event_bus(event_bus)
    
    # Store in app state
    app_state["event_bus"] = event_bus
    app_state["event_registry"] = event_registry
    
    # Set event bus in dependencies
    set_event_bus(event_bus)
    
    logger.info(f"Event system initialized with {len(event_registry.get_all_handlers())} handlers")
    
    # Initialize and start job worker
    worker = JobWorker(db_pool)
    worker.register_executor('import', ImportJobExecutor())
    worker.register_executor('sampling', SamplingJobExecutor())
    worker.register_executor('exploration', ExplorationExecutor(db_pool))
    worker.register_executor('sql_transform', SqlTransformExecutor())
    
    worker_task = asyncio.create_task(worker.start())
    app_state["worker_task"] = worker_task
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
    
    # Clear app state
    app_state["db_pool"] = None
    app_state["worker_task"] = None
    
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
app.include_router(users.router, prefix="/api")
app.include_router(search.router, prefix="/api")  # Must come before datasets to avoid route conflict
app.include_router(datasets.router, prefix="/api")
app.include_router(versioning.router, prefix="/api")
app.include_router(jobs.router, prefix="/api")
app.include_router(sampling.router, prefix="/api")
app.include_router(exploration.router, prefix="/api")
app.include_router(workbench.router, prefix="/api")
app.include_router(downloads.router, prefix="/api")


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "DSA Platform API", "version": "1.0.0"}


@app.get("/health")
async def health():
    """Health check endpoint."""
    db_pool = app_state.get("db_pool")
    worker_task = app_state.get("worker_task")
    
    return {
        "status": "healthy",
        "database": "connected" if db_pool and db_pool._pool is not None else "disconnected",
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