"""Application configuration"""
import os
from typing import Optional

# Security settings
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7

# Database settings
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/dsa")

# Application settings
APP_NAME = "DSA Platform"
VERSION = "2.0.0"
DEBUG = os.getenv("DEBUG", "True").lower() == "true"