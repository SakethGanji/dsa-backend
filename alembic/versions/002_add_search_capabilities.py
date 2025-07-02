"""Add search capabilities

Revision ID: 002
Revises: 001
Create Date: 2025-01-22

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None


def upgrade():
    """Add search extensions, indexes, and tables for dataset search"""
    
    # Create pg_trgm extension for fuzzy search
    op.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    
    # Create full-text search indexes on datasets table
    op.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_datasets_fts 
        ON datasets 
        USING gin(to_tsvector('english', COALESCE(name, '') || ' ' || COALESCE(description, '')))
    """))
    
    # Create trigram indexes for fuzzy search
    op.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_datasets_name_trgm 
        ON datasets 
        USING gin(name gin_trgm_ops)
    """))
    
    op.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_datasets_description_trgm 
        ON datasets 
        USING gin(description gin_trgm_ops)
    """))
    
    # Create index on tags for faster search
    op.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_tags_name_trgm 
        ON tags 
        USING gin(tag_name gin_trgm_ops)
    """))
    
    # Create search history table for tracking user searches
    op.create_table(
        'search_history',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('query', sa.Text(), nullable=True),
        sa.Column('filters', sa.JSON(), nullable=True),
        sa.Column('result_count', sa.Integer(), nullable=True),
        sa.Column('execution_time_ms', sa.Float(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE')
    )
    
    op.create_index('idx_search_history_user', 'search_history', ['user_id'])
    op.create_index('idx_search_history_created', 'search_history', ['created_at'])
    
    # Create saved searches table
    op.create_table(
        'saved_searches',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('query', sa.Text(), nullable=True),
        sa.Column('filters', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('user_id', 'name', name='uq_saved_searches_user_name')
    )
    
    op.create_index('idx_saved_searches_user', 'saved_searches', ['user_id'])
    
    # Add materialized view for faster facet computation (optional)
    op.execute(text("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS dataset_search_facets AS
        SELECT 
            d.id as dataset_id,
            d.name,
            d.created_by,
            u.soeid as created_by_name,
            date_trunc('year', d.created_at) as created_year,
            array_agg(DISTINCT t.tag_name) FILTER (WHERE t.tag_name IS NOT NULL) as tags,
            f.file_type,
            f.file_size,
            COUNT(DISTINCT dv.id) as version_count
        FROM datasets d
        LEFT JOIN users u ON d.created_by = u.id
        LEFT JOIN dataset_tags dt ON d.id = dt.dataset_id
        LEFT JOIN tags t ON dt.tag_id = t.id
        LEFT JOIN dataset_versions dv ON d.id = dv.dataset_id
        LEFT JOIN LATERAL (
            SELECT f.file_type, f.file_size
            FROM dataset_versions dv2
            LEFT JOIN dataset_version_files dvf ON dv2.id = dvf.version_id 
                AND dvf.component_type = 'primary'
            LEFT JOIN files f ON COALESCE(dvf.file_id, dv2.overlay_file_id) = f.id
            WHERE dv2.dataset_id = d.id
            ORDER BY dv2.version_number DESC
            LIMIT 1
        ) f ON true
        GROUP BY d.id, d.name, d.created_by, u.soeid, created_year, f.file_type, f.file_size
    """))
    
    # Create index on materialized view
    op.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_dataset_search_facets_dataset 
        ON dataset_search_facets(dataset_id)
    """))
    
    # Add composite indexes for common query patterns
    op.create_index(
        'idx_datasets_created_by_created_at',
        'datasets',
        ['created_by', 'created_at']
    )
    
    op.create_index(
        'idx_dataset_versions_dataset_version',
        'dataset_versions',
        ['dataset_id', 'version_number']
    )


def downgrade():
    """Remove search capabilities"""
    
    # Drop materialized view
    op.execute(text("DROP MATERIALIZED VIEW IF EXISTS dataset_search_facets"))
    
    # Drop tables
    op.drop_table('saved_searches')
    op.drop_table('search_history')
    
    # Drop indexes
    op.drop_index('idx_dataset_versions_dataset_version', 'dataset_versions')
    op.drop_index('idx_datasets_created_by_created_at', 'datasets')
    op.execute(text("DROP INDEX IF EXISTS idx_tags_name_trgm"))
    op.execute(text("DROP INDEX IF EXISTS idx_datasets_description_trgm"))
    op.execute(text("DROP INDEX IF EXISTS idx_datasets_name_trgm"))
    op.execute(text("DROP INDEX IF EXISTS idx_datasets_fts"))
    
    # Note: We don't drop pg_trgm extension as it might be used elsewhere