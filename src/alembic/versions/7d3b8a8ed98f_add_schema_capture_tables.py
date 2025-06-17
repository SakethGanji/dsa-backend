"""add_schema_capture_tables

Revision ID: 7d3b8a8ed98f
Revises: d32c3444e9f5
Create Date: 2025-06-17 18:17:01.351670

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7d3b8a8ed98f'
down_revision: Union[str, None] = 'd32c3444e9f5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create table for schema snapshots
    op.create_table('dataset_schema_versions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dataset_version_id', sa.Integer(), nullable=False),
        sa.Column('schema_json', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(), nullable=False, server_default=sa.text('NOW()')),
        sa.ForeignKeyConstraint(['dataset_version_id'], ['dataset_versions.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create index for version lookups
    op.create_index('idx_schema_versions_dataset', 'dataset_schema_versions', ['dataset_version_id'])


def downgrade() -> None:
    """Downgrade schema."""
    # Drop index
    op.drop_index('idx_schema_versions_dataset', 'dataset_schema_versions')
    
    # Drop table
    op.drop_table('dataset_schema_versions')
