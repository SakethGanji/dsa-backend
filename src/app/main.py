import logging # Add this import
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer
from fastapi.routing import APIRoute

from app.core.logging_config import logger # Use the configured logger
import app.core.logging_config # Ensures basicConfig is called

from app.users.auth import get_current_user_info, CurrentUser

# Import v2 routers
from app.users.routes import router as users_router
from app.datasets.routes import router as datasets_router
from app.explore.routes import router as explore_router
from app.sampling.routes import router as sampling_router
from app.jobs.routes import router as jobs_router

app = FastAPI(
    title="DSA Platform API v2",
    description="Git-like versioning system for dataset management",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,  # Important for cookies/auth
)

# centralized error handling
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.error(f"HTTPException: {exc.status_code} {exc.detail}", exc_info=True) # Log the exception
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global unhandled exception: {exc}", exc_info=True) # Log the exception
    # Unhandled exceptions become 500 Internal Server Error
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})

# Include v2 routers without /api prefix
app.include_router(users_router)  # /users endpoints
app.include_router(datasets_router)  # /datasets endpoints
app.include_router(explore_router)  # /datasets/{id}/commits/{hash}/explorations
app.include_router(sampling_router)  # /datasets/{id}/commits/{hash}/samples
app.include_router(jobs_router)  # /jobs endpoints

# Startup event to recover running jobs
@app.on_event("startup")
async def startup_event():
    """
    Recover any jobs that were running when the server shut down.
    
    Implementation Notes:
    In the v2 Git-like system with persistent job storage:
    1. Query analysis_runs table for jobs with status='running'
    2. Update their status to 'failed' with error message about server restart
    3. Optionally, restart recoverable jobs based on run_type
    
    Recovery Process:
    1. Get all running jobs from analysis_runs table
    2. For each job:
       - If it's an import job and files still exist, could restart
       - If it's a sampling/exploration job, mark as failed
       - Update status and error_message accordingly
    3. Log recovery results
    
    Database Query:
    UPDATE analysis_runs 
    SET status = 'failed',
        error_message = 'Job interrupted by server restart',
        completed_at = NOW()
    WHERE status = 'running';
    """
    try:
        logger.info("Starting up DSA Platform v2...")
        
        # HOLLOWED OUT - Implementation needed for new system
        # Dependencies to import:
        # - app.db.connection.AsyncSessionLocal
        # - Direct SQL update to mark running jobs as failed
        
        # Steps:
        # 1. Create database session
        # 2. Execute UPDATE query to mark running jobs as failed
        # 3. Log how many jobs were affected
        # 4. Optionally restart certain job types
        
        raise NotImplementedError(
            "Implement job recovery with analysis_runs table"
        )
            
    except NotImplementedError:
        logger.warning("Job recovery not implemented - skipping")
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}", exc_info=True)

@app.get("/")
async def root():
    return {
        "message": "DSA Platform API v2",
        "version": "2.0.0",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": "2.0.0"}

if __name__ == "__main__":
    import uvicorn
    
    # Configuration for exposing to internet
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",  # Listen on all network interfaces
        port=8000,       # Port to expose
        reload=True,     # Auto-reload on code changes (disable in production)
        log_level="info"
    )