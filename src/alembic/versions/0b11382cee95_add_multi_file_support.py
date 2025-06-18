"""add_multi_file_support

Revision ID: 0b11382cee95
Revises: 7d3b8a8ed98f
Create Date: 2025-06-17 18:37:40.687500

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0b11382cee95'
down_revision: Union[str, None] = '7d3b8a8ed98f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create junction table for version-file relationships
    op.create_table('dataset_version_files',
        sa.Column('version_id', sa.Integer(), nullable=False),
        sa.Column('file_id', sa.Integer(), nullable=False),
        sa.Column('component_type', sa.String(50), nullable=False),
        sa.Column('component_name', sa.Text(), nullable=True),
        sa.Column('component_index', sa.Integer(), nullable=True),
        sa.Column('metadata', sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(['version_id'], ['dataset_versions.id'], ),
        sa.ForeignKeyConstraint(['file_id'], ['files.id'], ),
        sa.PrimaryKeyConstraint('version_id', 'file_id')
    )
    
    # Create index for version lookups
    op.create_index('idx_version_files_version', 'dataset_version_files', ['version_id'])
    
    # Migrate existing data: create entries in version_files for existing versions
    # This ensures backward compatibility
    op.execute("""
        INSERT INTO dataset_version_files (version_id, file_id, component_type, component_name)
        SELECT id, file_id, 'primary', 'main'
        FROM dataset_versions
        WHERE file_id IS NOT NULL
    """)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop index
    op.drop_index('idx_version_files_version', 'dataset_version_files')
    
    # Drop table
    op.drop_table('dataset_version_files')
