from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.users.routes import router as users_router
from app.datasets.routes import router as datasets_router
from app.explore.routes import router as explore_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# centralized error handling
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    # Unhandled exceptions become 500 Internal Server Error
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


app.include_router(users_router)
app.include_router(datasets_router)
app.include_router(explore_router)
