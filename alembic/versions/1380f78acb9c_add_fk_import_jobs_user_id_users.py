"""add fk import_jobs user_id users

Revision ID: 1380f78acb9c
Revises: f5c96adb2254
Create Date: 2026-01-30 01:17:32.757771

"""
from alembic import op
import sqlalchemy as sa

revision = '1380f78acb9c'
down_revision = 'f5c96adb2254'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_foreign_key(
        "fk_import_jobs_user_id_users",
        "import_jobs",
        "users",
        ["user_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_import_jobs_user_id_users",
        "import_jobs",
        type_="foreignkey",
    )
