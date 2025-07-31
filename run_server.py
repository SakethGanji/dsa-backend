#!/usr/bin/env python3
"""Run the FastAPI server on the machine's IP address."""

import uvicorn
import socket

def get_local_ip():
    """Get the local IP address of the machine."""
    try:
        # Create a socket to determine the local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

if __name__ == "__main__":
    host = get_local_ip()
    port = 8000
    
    print(f"Starting server on http://{host}:{port}")
    print("You can access the API documentation at:")
    print(f"  - http://{host}:{port}/docs (Swagger UI)")
    print(f"  - http://{host}:{port}/redoc (ReDoc)")
    
    uvicorn.run(
        "src.main:app",
        host=host,
        port=port,
        reload=True  # Enable auto-reload for development
    )