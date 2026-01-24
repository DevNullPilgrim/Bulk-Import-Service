"""add users and idempotency

Revision ID: f5c96adb2254
Revises: a72fc880f246
Create Date: 2026-01-24 21:32:45.862057

"""
from alembic import op
import sqlalchemy as sa

revision = 'f5c96adb2254'
down_revision = 'a72fc880f246'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'users',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('email', sa.String(length=320), nullable=False),
        sa.Column('hashed_password', sa.String(length=255), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True),
                  server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('email'),
    )
    op.create_index('ix_users_email', 'users', ['email'], unique=False)

    op.add_column('import_jobs', sa.Column(
        'user_id', sa.Uuid(), nullable=False))
    op.add_column('import_jobs', sa.Column(
        'idempotency_key', sa.String(length=128), nullable=False))
    op.create_index('ix_import_jobs_user_id', 'import_jobs',
                    ['user_id'], unique=False)
    op.create_unique_constraint(
        'uq_import_jobs_user_id_idempotency_key',
        'import_jobs',
        ['user_id', 'idempotency_key'],
    )


def downgrade() -> None:
    op.drop_constraint('uq_import_jobs_user_id_idempotency_key',
                       'import_jobs', type_='unique')
    op.drop_index('ix_import_jobs_user_id', table_name='import_jobs')
    op.drop_column('import_jobs', 'idempotency_key')
    op.drop_column('import_jobs', 'user_id')

    op.drop_index('ix_users_email', table_name='users')
    op.drop_table('users')
