import enum
import uuid
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy import DateTime, Enum, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class JobStatus(str, enum.Enum):
    pending = 'pending'
    processing = 'processing'
    done = 'done'
    failed = 'failed'


class ImportMode(str, enum.Enum):
    insert_only = 'insert_only'
    upsert = 'upsert'


class ImportJob(Base):
    __tablename__ = 'import_jobs'

    id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4

    )
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name='job_status'),
        default=JobStatus.pending,
        nullable=False,
    )
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    s3_key: Mapped[str] = mapped_column(String(1024), nullable=False)

    total_rows: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False)
    processed_rows: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=sa.func.now(),
        nullable=False,
    )
    mode: Mapped[ImportMode] = mapped_column(
        Enum(ImportMode, name='import_mode'),
        default=ImportMode.insert_only,
        nullable=False
    )
    error_report_object_key: Mapped[str | None] = mapped_column(
        String(1024),
        nullable=True
    )
    error_count: Mapped[Integer] = mapped_column(
        Integer,
        default=0,
        nullable=False
    )
