"""Initial schema capture

Revision ID: 340ade7b7bd4
Revises: 
Create Date: 2025-06-17 16:23:32.205878

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '340ade7b7bd4'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # This migration assumes the database already has the current schema
    # We'll mark it as already applied
    pass


def downgrade() -> None:
    """Downgrade schema."""
    # No downgrade for initial schema
    pass
