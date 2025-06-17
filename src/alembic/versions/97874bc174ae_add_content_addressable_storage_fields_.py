"""Add content addressable storage fields to files table

Revision ID: 97874bc174ae
Revises: 340ade7b7bd4
Create Date: 2025-06-17 16:29:42.755940

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '97874bc174ae'
down_revision: Union[str, None] = '340ade7b7bd4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add content_hash column for content-addressable storage
    op.add_column('files', sa.Column('content_hash', sa.String(64), nullable=True))
    
    # Add reference_count for garbage collection
    op.add_column('files', sa.Column('reference_count', sa.BigInteger(), nullable=False, server_default='0'))
    
    # Add compression_type
    op.add_column('files', sa.Column('compression_type', sa.String(50), nullable=True))
    
    # Add metadata column
    op.add_column('files', sa.Column('metadata', sa.JSON(), nullable=True))
    
    # Create unique index on content_hash
    op.create_index('idx_files_content_hash', 'files', ['content_hash'], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop index
    op.drop_index('idx_files_content_hash', 'files')
    
    # Remove columns
    op.drop_column('files', 'metadata')
    op.drop_column('files', 'compression_type')
    op.drop_column('files', 'reference_count')
    op.drop_column('files', 'content_hash')
