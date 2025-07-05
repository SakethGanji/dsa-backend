"""Test utilities and helper functions."""

from typing import Dict, Any
import json
from datetime import datetime, timedelta
import jwt
from passlib.context import CryptContext


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def create_test_token(
    soeid: str,
    role_id: int,
    role_name: str = None,
    expires_in_minutes: int = 30,
    secret_key: str = "test-secret-key-for-testing-only"
) -> str:
    """Create a test JWT token."""
    payload = {
        "sub": soeid,
        "role_id": role_id,
        "exp": datetime.utcnow() + timedelta(minutes=expires_in_minutes)
    }
    if role_name:
        payload["role_name"] = role_name
    
    return jwt.encode(payload, secret_key, algorithm="HS256")


def create_expired_token(
    soeid: str,
    role_id: int,
    secret_key: str = "test-secret-key-for-testing-only"
) -> str:
    """Create an expired JWT token for testing."""
    payload = {
        "sub": soeid,
        "role_id": role_id,
        "exp": datetime.utcnow() - timedelta(minutes=1)  # Expired 1 minute ago
    }
    
    return jwt.encode(payload, secret_key, algorithm="HS256")


def hash_password(password: str) -> str:
    """Hash a password for testing."""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against a hash."""
    return pwd_context.verify(plain_password, hashed_password)


async def create_test_commit(db_session, dataset_id: int, author_id: int) -> str:
    """Create a test commit with some data."""
    import hashlib
    
    # Create test data
    test_rows = [
        {"row_hash": hashlib.sha256(b"row1").hexdigest(), "data": json.dumps({"id": 1, "name": "test1"})},
        {"row_hash": hashlib.sha256(b"row2").hexdigest(), "data": json.dumps({"id": 2, "name": "test2"})}
    ]
    
    # Insert rows
    for row in test_rows:
        await db_session.execute("""
            INSERT INTO dsa_core.rows (row_hash, data)
            VALUES ($1, $2)
            ON CONFLICT (row_hash) DO NOTHING
        """, row["row_hash"], row["data"])
    
    # Create commit
    commit_data = {
        "dataset_id": dataset_id,
        "parent_commit_id": None,
        "manifest": [(f"default:{i}", row["row_hash"]) for i, row in enumerate(test_rows)],
        "message": "Test commit",
        "author_id": author_id
    }
    
    commit_id = hashlib.sha256(
        json.dumps(commit_data, sort_keys=True).encode()
    ).hexdigest()
    
    await db_session.execute("""
        INSERT INTO dsa_core.commits (commit_id, dataset_id, parent_commit_id, message, author_id)
        VALUES ($1, $2, $3, $4, $5)
    """, commit_id, dataset_id, None, "Test commit", author_id)
    
    # Insert manifest
    for logical_row_id, row_hash in commit_data["manifest"]:
        await db_session.execute("""
            INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
            VALUES ($1, $2, $3)
        """, commit_id, logical_row_id, row_hash)
    
    # Update ref
    await db_session.execute("""
        UPDATE dsa_core.refs
        SET commit_id = $1
        WHERE dataset_id = $2 AND name = 'main'
    """, commit_id, dataset_id)
    
    return commit_id


class APITestClient:
    """Helper class for API testing."""
    
    def __init__(self, client):
        self.client = client
        self._auth_headers = None
    
    async def login(self, soeid: str, password: str) -> Dict[str, Any]:
        """Login and store auth headers."""
        response = await self.client.post(
            "/api/users/login",
            data={"username": soeid, "password": password}
        )
        if response.status_code == 200:
            token = response.json()["access_token"]
            self._auth_headers = {"Authorization": f"Bearer {token}"}
        return response.json()
    
    @property
    def auth_headers(self) -> Dict[str, str]:
        """Get stored auth headers."""
        return self._auth_headers or {}
    
    async def create_dataset(self, name: str, description: str = "") -> Dict[str, Any]:
        """Create a dataset with auth."""
        response = await self.client.post(
            "/api/datasets/",
            json={"name": name, "description": description},
            headers=self.auth_headers
        )
        return response.json()
    
    async def grant_permission(
        self, 
        dataset_id: int, 
        user_id: int, 
        permission_type: str
    ) -> Dict[str, Any]:
        """Grant permission on dataset."""
        response = await self.client.post(
            f"/api/datasets/{dataset_id}/permissions",
            json={"user_id": user_id, "permission_type": permission_type},
            headers=self.auth_headers
        )
        return response.json()