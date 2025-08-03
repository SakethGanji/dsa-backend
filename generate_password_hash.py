#!/usr/bin/env python3
"""Generate password hash using the same method as the application"""

from passlib.context import CryptContext

# Create context with bcrypt
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Hash the password
password = "password"
hashed = pwd_context.hash(password)

print(f"Password: {password}")
print(f"Hash: {hashed}")

# Verify it works
verified = pwd_context.verify(password, hashed)
print(f"Verification: {verified}")