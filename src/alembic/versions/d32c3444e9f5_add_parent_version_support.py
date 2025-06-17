"""add_parent_version_support

Revision ID: d32c3444e9f5
Revises: 97874bc174ae
Create Date: 2025-06-17 17:33:52.419133

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd32c3444e9f5'
down_revision: Union[str, None] = '97874bc174ae'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add parent reference and message to dataset_versions
    op.add_column('dataset_versions', 
        sa.Column('parent_version_id', sa.Integer(), 
                  sa.ForeignKey('dataset_versions.id'), nullable=True))
    op.add_column('dataset_versions', 
        sa.Column('message', sa.Text(), nullable=True))
    op.add_column('dataset_versions', 
        sa.Column('overlay_file_id', sa.Integer(), 
                  sa.ForeignKey('files.id'), nullable=True))
    
    # Add index for parent lookups
    op.create_index('idx_dataset_versions_parent', 
                    'dataset_versions', ['parent_version_id'])


def downgrade() -> None:
    """Downgrade schema."""
    # Drop index
    op.drop_index('idx_dataset_versions_parent', 'dataset_versions')
    
    # Drop columns
    op.drop_column('dataset_versions', 'overlay_file_id')
    op.drop_column('dataset_versions', 'message')
    op.drop_column('dataset_versions', 'parent_version_id')
