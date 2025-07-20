"""User command objects."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class CreateUserCommand:
    """Command to create a new user."""
    soeid: str
    password: str
    role_id: int
    created_by: int  # User creating this user


@dataclass
class CreateUserPublicCommand:
    """Command to create a user via public endpoint (for testing)."""
    soeid: str
    password: str
    role_id: int = 2  # Default to regular user


@dataclass
class UpdateUserCommand:
    """Command to update user information."""
    user_id: int  # Must be first for decorator - this is the requesting user (admin)
    target_user_id: int
    soeid: Optional[str] = None
    password: Optional[str] = None
    role_id: Optional[int] = None


@dataclass
class DeleteUserCommand:
    """Command to delete a user."""
    user_id: int  # Must be first for decorator - this is the requesting user (admin)
    target_user_id: int


@dataclass
class LoginCommand:
    """Command to login a user."""
    soeid: str
    password: str


@dataclass
class ListUsersCommand:
    """Command to list users."""
    user_id: int  # Must be first for decorator
    offset: int = 0
    limit: int = 100
    search: Optional[str] = None
    role_id: Optional[int] = None
    sort_by: Optional[str] = "created_at"
    sort_order: Optional[str] = "desc"


@dataclass
class SuspendUserCommand:
    """Command to suspend a user."""
    user_id: int
    requesting_user_id: int
    reason: Optional[str] = None


@dataclass
class ReactivateUserCommand:
    """Command to reactivate a suspended user."""
    user_id: int
    requesting_user_id: int