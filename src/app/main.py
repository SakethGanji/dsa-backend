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