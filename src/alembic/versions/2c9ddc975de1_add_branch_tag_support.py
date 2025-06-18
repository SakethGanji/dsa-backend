"""add_branch_tag_support

Revision ID: 2c9ddc975de1
Revises: 0b11382cee95
Create Date: 2025-06-17 19:12:08.609734

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2c9ddc975de1'
down_revision: Union[str, None] = '0b11382cee95'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create pointers table for branches and tags
    op.create_table('dataset_pointers',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dataset_id', sa.Integer(), nullable=False),
        sa.Column('pointer_name', sa.String(255), nullable=False),
        sa.Column('dataset_version_id', sa.Integer(), nullable=False),
        sa.Column('is_tag', sa.Boolean(), nullable=False, server_default=sa.text('FALSE')),
        sa.Column('created_at', sa.TIMESTAMP(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.TIMESTAMP(), nullable=False, server_default=sa.text('NOW()')),
        sa.ForeignKeyConstraint(['dataset_id'], ['datasets.id'], ),
        sa.ForeignKeyConstraint(['dataset_version_id'], ['dataset_versions.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('dataset_id', 'pointer_name')
    )
    
    # Create index for dataset lookups
    op.create_index('idx_pointers_dataset', 'dataset_pointers', ['dataset_id'])
    
    # Create default 'main' branch for existing datasets with versions
    op.execute("""
        INSERT INTO dataset_pointers (dataset_id, pointer_name, dataset_version_id, is_tag)
        SELECT DISTINCT dv.dataset_id, 'main', 
               (SELECT id FROM dataset_versions 
                WHERE dataset_id = dv.dataset_id 
                ORDER BY version_number DESC LIMIT 1), 
               FALSE
        FROM dataset_versions dv
        WHERE NOT EXISTS (
            SELECT 1 FROM dataset_pointers dp 
            WHERE dp.dataset_id = dv.dataset_id AND dp.pointer_name = 'main'
        )
    """)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop index
    op.drop_index('idx_pointers_dataset', 'dataset_pointers')
    
    # Drop table
    op.drop_table('dataset_pointers')
