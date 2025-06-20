"""simplify_versioning_remove_branches

Revision ID: 87e2508b3c7d
Revises: ef2e4901429d
Create Date: 2025-06-19 15:22:06.380721

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '87e2508b3c7d'
down_revision: Union[str, None] = 'ef2e4901429d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Remove complex versioning features to simplify to linear versioning."""
    # Get connection
    conn = op.get_bind()
    
    # Check if dataset_pointers table exists and drop it
    result = conn.execute(sa.text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'dataset_pointers'
        );
    """))
    if result.scalar():
        # Drop indexes first
        conn.execute(sa.text("DROP INDEX IF EXISTS idx_pointers_dataset;"))
        # Drop table
        conn.execute(sa.text("DROP TABLE IF EXISTS dataset_pointers CASCADE;"))
    
    # Check if transformations table exists and drop it
    result = conn.execute(sa.text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'transformations'
        );
    """))
    if result.scalar():
        conn.execute(sa.text("DROP TABLE IF EXISTS transformations CASCADE;"))
    
    # Check if parent version columns exist and drop them
    result = conn.execute(sa.text("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'dataset_versions' 
        AND column_name IN ('parent_version_id', 'message', 'overlay_file_id');
    """))
    columns_to_drop = [row[0] for row in result]
    
    # Drop index if exists
    if 'parent_version_id' in columns_to_drop:
        conn.execute(sa.text("DROP INDEX IF EXISTS idx_dataset_versions_parent;"))
    
    # Drop columns
    for column in columns_to_drop:
        conn.execute(sa.text(f"ALTER TABLE dataset_versions DROP COLUMN IF EXISTS {column};"))
    
    # Create the new version_tags table
    op.create_table('version_tags',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dataset_id', sa.Integer(), nullable=False),
        sa.Column('tag_name', sa.String(255), nullable=False),
        sa.Column('dataset_version_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['dataset_id'], ['datasets.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['dataset_version_id'], ['dataset_versions.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('dataset_id', 'tag_name')
    )
    
    # Create index for version tag lookups
    op.create_index('idx_version_tags_lookup', 'version_tags', ['dataset_id', 'tag_name'])


def downgrade() -> None:
    """Re-add complex versioning features."""
    # Drop the version_tags table
    op.drop_index('idx_version_tags_lookup', 'version_tags')
    op.drop_table('version_tags')
    
    # Re-add parent version support columns
    op.add_column('dataset_versions', 
        sa.Column('parent_version_id', sa.Integer(), 
                  sa.ForeignKey('dataset_versions.id'), nullable=True))
    op.add_column('dataset_versions', 
        sa.Column('message', sa.Text(), nullable=True))
    op.add_column('dataset_versions', 
        sa.Column('overlay_file_id', sa.Integer(), 
                  sa.ForeignKey('files.id'), nullable=True))
    
    # Re-add index for parent lookups
    op.create_index('idx_dataset_versions_parent', 
                    'dataset_versions', ['parent_version_id'])
    
    # Re-create pointers table for branches and tags
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
    
    # Re-create index for dataset lookups
    op.create_index('idx_pointers_dataset', 'dataset_pointers', ['dataset_id'])