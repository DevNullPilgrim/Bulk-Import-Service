import uuid
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Customer(Base):
    __tablename__ = 'customers'

    id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    email: Mapped[str] = mapped_column(
        String(320),
        nullable=False,
        unique=True)
    first_name: Mapped[str | None] = mapped_column(
        String(150),
        nullable=True)
    last_name: Mapped[str | None] = mapped_column(
        String(150),
        nullable=True)
    phone: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True)
    city: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=sa.func.now(),
        nullable=False
    )
    update_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False
    )
