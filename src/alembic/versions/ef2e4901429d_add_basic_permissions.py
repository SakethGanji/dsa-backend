"""add_basic_permissions

Revision ID: ef2e4901429d
Revises: 2c9ddc975de1
Create Date: 2025-06-17 19:25:00.434399

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ef2e4901429d'
down_revision: Union[str, None] = '2c9ddc975de1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create permissions table
    op.create_table('permissions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('resource_type', sa.String(50), nullable=False),
        sa.Column('resource_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('permission_type', sa.String(20), nullable=False),
        sa.Column('granted_at', sa.TIMESTAMP(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('granted_by', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.ForeignKeyConstraint(['granted_by'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('resource_type', 'resource_id', 'user_id', 'permission_type')
    )
    
    # Create index for permission lookups
    op.create_index('idx_permissions_lookup', 'permissions', 
                    ['resource_type', 'resource_id', 'user_id'])
    
    # Grant admin permissions to dataset creators for existing datasets
    op.execute("""
        INSERT INTO permissions (resource_type, resource_id, user_id, permission_type, granted_by)
        SELECT 'dataset', id, created_by, 'admin', created_by
        FROM datasets
        WHERE NOT EXISTS (
            SELECT 1 FROM permissions p 
            WHERE p.resource_type = 'dataset' 
            AND p.resource_id = datasets.id 
            AND p.user_id = datasets.created_by
            AND p.permission_type = 'admin'
        )
    """)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop index
    op.drop_index('idx_permissions_lookup', 'permissions')
    
    # Drop table
    op.drop_table('permissions')
