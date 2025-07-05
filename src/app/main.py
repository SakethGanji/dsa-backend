import logging
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer
from fastapi.routing import APIRoute

from app.core.logging_config import logger
import app.core.logging_config  # Ensures basicConfig is called

from app.users.auth import get_current_user_info, CurrentUser

# Import only users router - other modules removed for clean slate
from app.users.routes import router as users_router

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
    allow_credentials=True,
)

# Centralized error handling
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.error(f"HTTPException: {exc.status_code} {exc.detail}", exc_info=True)
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global unhandled exception: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})

# Include only users router for now
app.include_router(users_router)  # /users endpoints

# Startup event for future job recovery
@app.on_event("startup")
async def startup_event():
    """
    Initialize application and recover any interrupted jobs.
    
    In the new Git-like system:
    - Check analysis_runs table for jobs with status='running'
    - Mark them as failed with appropriate error message
    - Optionally restart recoverable jobs
    """
    try:
        logger.info("Starting up DSA Platform v2...")
        # Job recovery to be implemented with new backend
        logger.info("Application startup complete")
            
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}", exc_info=True)

@app.get("/")
async def root():
    return {
        "message": "DSA Platform API v2",
        "version": "2.0.0",
        "docs": "/docs",
        "status": "Clean slate - ready for new implementation"
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": "2.0.0"}

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )