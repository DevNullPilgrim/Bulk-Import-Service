import enum
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy import DateTime, Enum, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

datetime.now(timezone.utc)


class JobStatus(str, enum.Enum):
    pending = 'peding'
    processing = 'processing'
    done = 'done'
    failed = 'failed'


class ImportJob(Base):
    __tablename__ = 'import _jobs'

    id: Mapped[int] = mapped_column(primary_key=True)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name='job_status'),
        default=JobStatus.pending,
        nullable=False,
    )
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    s3_key: Mapped[str] = mapped_column(String(1024), nullable=False)

    total_row: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processed_row: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False)

    error: Mapped[str | None] = mapped_column(Text, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=sa.func.now(),
        nullable=False,
    )
