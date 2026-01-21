"""add import_jobs error report fields

Revision ID: a72fc880f246
Revises: f934da49f99e
Create Date: 2026-01-21 23:52:26.232792

"""
from alembic import op
import sqlalchemy as sa

revision = 'a72fc880f246'
down_revision = 'f934da49f99e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "import_jobs",
        sa.Column("error_report_object_key",
                  sa.String(length=1024), nullable=True),
    )
    op.add_column(
        "import_jobs",
        sa.Column("error_count", sa.Integer(),
                  nullable=False, server_default="0"),
    )
    op.alter_column("import_jobs", "error_count", server_default=None)


def downgrade() -> None:
    op.drop_column("import_jobs", "error_count")
    op.drop_column("import_jobs", "error_report_object_key")
