import logging # Add this import
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer
from fastapi.routing import APIRoute

from app.core.logging_config import logger # Use the configured logger
import app.core.logging_config # Ensures basicConfig is called

from app.users.auth import get_current_user_info, CurrentUser

from app.users.routes import router as users_router
from app.datasets.routes import router as datasets_router
from app.explore.routes import router as explore_router
from app.sampling.routes import router as sampling_router

app = FastAPI()

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

app.include_router(users_router)
app.include_router(datasets_router)
app.include_router(explore_router)
app.include_router(sampling_router)

# Startup event to recover running jobs
@app.on_event("startup")
async def startup_event():
    """Recover any sampling jobs that were running when the server shut down"""
    try:
        logger.info("Starting up application...")
        
        # Get the sampling service instance with database session
        from app.db.connection import AsyncSessionLocal
        from app.sampling.service import SamplingService
        from app.datasets.repository import DatasetsRepository
        from app.sampling.repository import SamplingRepository
        from app.storage.factory import StorageFactory
        
        async with AsyncSessionLocal() as session:
            datasets_repository = DatasetsRepository(session)
            sampling_repo = SamplingRepository()
            storage_backend = StorageFactory.get_instance()
            
            # Create service with database session to enable recovery
            service = SamplingService(
                datasets_repository, 
                sampling_repo, 
                storage_backend, 
                db_session=session
            )
            
            # Recover any running jobs
            await service.recover_running_jobs()
            
        logger.info("Application startup complete")
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}", exc_info=True)

@app.get("/")
async def root():
    return {"message": "Data Science API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

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