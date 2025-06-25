"""Remove storage_type column and use self-describing URIs

Revision ID: 003
Revises: 002_add_search_capabilities
Create Date: 2025-06-25

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '003'
down_revision = '002_add_search_capabilities'
branch_labels = None
depends_on = None


def upgrade():
    """Remove storage_type column and convert to URI-based file paths."""
    
    # Step 1: Update existing file_path values to be full URIs
    # Convert existing paths to file:// URIs
    op.execute("""
        UPDATE files 
        SET file_path = 
            CASE 
                WHEN storage_type = 'local' THEN 'file://' || file_path
                WHEN storage_type = 'filesystem' THEN 'file://' || file_path
                WHEN storage_type = 's3' THEN 's3://' || file_path
                ELSE file_path  -- Keep as-is if storage_type is something else
            END
        WHERE file_path NOT LIKE 'file://%' 
          AND file_path NOT LIKE 's3://%'
          AND file_path NOT LIKE 'http://%'
          AND file_path NOT LIKE 'https://%'
          AND file_path IS NOT NULL
    """)
    
    # Step 2: Update any NULL file_path values to have a default
    op.execute("""
        UPDATE files 
        SET file_path = 'file:///data/artifacts/' || content_hash
        WHERE file_path IS NULL AND content_hash IS NOT NULL
    """)
    
    # Step 3: Make file_path NOT NULL
    op.alter_column('files', 'file_path',
                    existing_type=sa.TEXT(),
                    nullable=False)
    
    # Step 4: Drop the storage_type column
    op.drop_column('files', 'storage_type')
    
    # Step 5: Add a check constraint to ensure file_path is a valid URI
    op.create_check_constraint(
        'file_path_is_uri',
        'files',
        sa.text("""
            file_path LIKE 'file://%' OR 
            file_path LIKE 's3://%' OR 
            file_path LIKE 'gs://%' OR 
            file_path LIKE 'azure://%' OR
            file_path LIKE 'http://%' OR
            file_path LIKE 'https://%'
        """)
    )
    
    # Step 6: Create an index on the URI scheme for efficient backend selection
    op.create_index(
        'idx_files_uri_scheme',
        'files',
        [sa.text("substring(file_path from '^[^:]+:')")]
    )


def downgrade():
    """Restore storage_type column."""
    
    # Drop the new constraints and index
    op.drop_index('idx_files_uri_scheme', 'files')
    op.drop_constraint('file_path_is_uri', 'files', type_='check')
    
    # Add storage_type column back
    op.add_column('files', sa.Column('storage_type', sa.VARCHAR(50), nullable=True))
    
    # Populate storage_type based on URI scheme
    op.execute("""
        UPDATE files 
        SET storage_type = 
            CASE 
                WHEN file_path LIKE 'file://%' THEN 'filesystem'
                WHEN file_path LIKE 's3://%' THEN 's3'
                WHEN file_path LIKE 'gs://%' THEN 'gcs'
                WHEN file_path LIKE 'azure://%' THEN 'azure'
                ELSE 'filesystem'  -- Default
            END
    """)
    
    # Make storage_type NOT NULL
    op.alter_column('files', 'storage_type',
                    existing_type=sa.VARCHAR(50),
                    nullable=False)
    
    # Remove URI prefixes from file_path
    op.execute("""
        UPDATE files 
        SET file_path = 
            CASE 
                WHEN file_path LIKE 'file:///%' THEN substring(file_path from 8)
                WHEN file_path LIKE 's3://%' THEN substring(file_path from 6)
                WHEN file_path LIKE 'gs://%' THEN substring(file_path from 6)
                WHEN file_path LIKE 'azure://%' THEN substring(file_path from 9)
                ELSE file_path
            END
    """)
    
    # Make file_path nullable again
    op.alter_column('files', 'file_path',
                    existing_type=sa.TEXT(),
                    nullable=True)