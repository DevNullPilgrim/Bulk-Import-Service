import csv
import io
import os
import time
import uuid
from typing import Iterator

import sqlalchemy as sa
from celery import Celery
from celery.utils.log import get_task_logger
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.config import settings
from app.db.session import SessionLocal
from app.models.customer import Customer
from app.models.import_job import ImportJob, ImportMode, JobStatus
from app.storage.s3 import get_bytes, put_bytes
from worker.errors_report import ErrorRow, build_errors_csv

app = Celery(
    'bulk_import',
    broker=settings.redis_url,
    backend=settings.redis_url,
)

logger = get_task_logger(__name__)

PROGRESS_EVERY = int(os.getenv('PROGRESS_EVERY', '50'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '500'))
SLOW_MS = int(os.getenv('IMPORT_SLOW_MS', '0'))


@app.task(name='ping')
def ping():
    return 'pong'


def iter_csv_rows(data: bytes) -> Iterator[list[str]]:
    with io.TextIOWrapper(
        io.BytesIO(data),
        encoding='utf-8-sig',
        errors='replace',
        newline='',
    ) as text:
        reader = csv.reader(text)
        next(reader, None)
        yield from reader


def count_csv_rows(data: bytes) -> int:
    return sum(1 for _ in iter_csv_rows(data))


def _update_job(db, job_uuid: uuid.UUID, **fields) -> None:
    db.execute(
        update(ImportJob)
        .where(ImportJob.id == job_uuid)
        .values(**fields)
    )
    db.commit()


def _norm(s: str | None) -> str | None:
    if s is None:
        return None
    s = s.strip()
    return s or None


def parse_customer_row(row: list[str], row_num: int) -> tuple[dict | None, str | None]:
    if not row:
        return None, f'row {row_num}: empty row'

    email = _norm(row[0])
    if not email:
        return None, f'row {row_num}: empty email'

    if '@' not in email or '.' not in email.split('@')[-1]:
        return None, f'row {row_num}: invalid email "{email}"'

    return {
        'email': email,
        'first_name': _norm(row[1]) if len(row) > 1 else None,
        'last_name': _norm(row[2]) if len(row) > 2 else None,
        'phone': _norm(row[3]) if len(row) > 3 else None,
        'city': _norm(row[4]) if len(row) > 4 else None,
    }, None


class BatchBuffer:
    def __init__(self, size: int):
        self.size = size
        self.rows: list[dict] = []
        self.row_nums: list[int] = []

    def add(self, payload: dict, row_num: int) -> None:
        self.rows.append(payload)
        self.row_nums.append(row_num)

    def full(self) -> bool:
        return len(self.rows) >= self.size

    def clear(self) -> None:
        self.rows.clear()
        self.row_nums.clear()


class InsertOnlyFlusher:
    def flush(self,
              db,
              buffer: BatchBuffer,
              errors: list[str],
              error_rows: list[ErrorRow]) -> None:
        if not buffer.rows:
            return

        emails = [r['email'] for r in buffer.rows]
        existing = set(
            db.execute(
                select(Customer.email).where(Customer.email.in_(emails))
            ).scalars().all()
        )

        to_insert = []
        for payload, rn in zip(buffer.rows, buffer.row_nums):
            if payload['email'] in existing:
                msg = f'email already exists {payload['email']}'
                errors.append(
                    f'row {rn}: {msg}')
                error_rows.append(ErrorRow(row=rn,
                                           error=msg,
                                           raw=payload['email']))
            else:
                to_insert.append(payload)

        if to_insert:
            db.execute(sa.insert(Customer).values(to_insert))

        db.commit()


class UpsertFlusher:
    def flush(self,
              db,
              buffer: BatchBuffer,
              errors: list[str],
              error_rows: list[ErrorRow],) -> None:
        if not buffer.rows:
            return

        stmt = pg_insert(Customer).values(buffer.rows)

        # ВАЖНО: колонка называется update_at (см. модель + миграцию)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Customer.email],
            set_={
                'first_name': stmt.excluded.first_name,
                'last_name': stmt.excluded.last_name,
                'phone': stmt.excluded.phone,
                'city': stmt.excluded.city,
                'update_at': sa.func.now(),
            },
        )

        db.execute(stmt)
        db.commit()


def get_flusher(mode: ImportMode):
    return InsertOnlyFlusher() if mode == ImportMode.insert_only else UpsertFlusher()


def _short_error_summary(errors: list[str], limit: int = 3) -> str:
    if not errors:
        return ''
    head = ' | '.join(errors[:limit])
    if len(errors) > limit:
        return f'errors: {len(errors)}; first: {head} ...'
    return f'errors: {len(errors)}; first: {head}'


def parse_job_id(job_id: str) -> uuid.UUID | None:
    try:
        return uuid.UUID(job_id)
    except ValueError:
        return None


def load_job_meta(db, job_uuid: uuid.UUID) -> tuple[str, ImportMode] | None:
    row = db.execute(
        select(ImportJob.s3_key, ImportJob.mode).where(
            ImportJob.id == job_uuid)
    ).one_or_none()
    if row is None:
        return None
    return row[0], row[1]


def mark_failed(db, job_uuid: uuid.UUID, err: Exception) -> None:
    db.rollback()
    _update_job(
        db,
        job_uuid,
        status=JobStatus.failed,
        error=f'{type(err).__name__}: {err}',
    )


def process_csv(db,
                job_uuid: uuid.UUID,
                data: bytes,
                flusher) -> tuple[int, list[str], list[ErrorRow]]:
    processed = 0
    errors: list[str] = []
    error_rows: list[ErrorRow] = []
    seen_emails: set[str] = set()  # дубли в файле
    buffer = BatchBuffer(BATCH_SIZE)

    for row_num, row in enumerate(iter_csv_rows(data), start=1):
        processed += 1

        payload, err = parse_customer_row(row, row_num)
        if err:
            errors.append(err)
            error_rows.append(ErrorRow(row=row_num,
                                       error=err,
                                       raw=','.join(row)))
        else:
            email = payload['email']
            if email in seen_emails:
                msg = f'duplicate email "{email}" in file'
                errors.append(f'row {row_num}: {msg}')
                error_rows.append(ErrorRow(row=row_num,
                                           error=msg,
                                           raw=','.join(row)))
            else:
                seen_emails.add(email)
                buffer.add(payload, row_num)

        if SLOW_MS:
            time.sleep(SLOW_MS / 1000)

        if processed % PROGRESS_EVERY == 0:
            _update_job(db, job_uuid, processed_rows=processed)

        if buffer.full():
            flusher.flush(db, buffer, errors, error_rows)
            buffer.clear()

    flusher.flush(db, buffer, errors, error_rows)
    buffer.clear()

    return processed, errors, error_rows


def run_import(db, job_uuid: uuid.UUID, s3_key: str, mode: ImportMode) -> None:
    _update_job(db, job_uuid, status=JobStatus.processing,
                error=None, processed_rows=0)

    data = get_bytes(s3_key)
    total = count_csv_rows(data)
    _update_job(db, job_uuid, total_rows=total, processed_rows=0)

    flusher = get_flusher(mode)
    processed, errors, error_rows = process_csv(db, job_uuid, data, flusher)

    report_key = None
    if error_rows:
        errors_bytes = build_errors_csv(error_rows)
        report_key = put_bytes(errors_bytes, filename=f'errors_{job_uuid}.csv')

    final_status = JobStatus.done if not errors else JobStatus.failed
    final_error = None if not errors else _short_error_summary(errors)

    _update_job(
        db,
        job_uuid,
        processed_rows=processed,
        status=final_status,
        error=final_error,
        error_report_object_key=report_key,
        error_count=len(error_rows),
    )


@app.task(name='process_import', bind=True)
def process_import(self, job_id: str) -> str:
    job_uuid = parse_job_id(job_id)
    if not job_uuid:
        logger.error('Invalid job id: %s', job_id)
        return 'bad_id'

    with SessionLocal() as db:
        meta = load_job_meta(db, job_uuid)
        if not meta:
            logger.error('ImportJob not found: %s', job_id)
            return 'not_found'

        s3_key, mode = meta

        try:
            run_import(db, job_uuid, s3_key, mode)
            return 'ok'
        except Exception as e:
            mark_failed(db, job_uuid, e)
            logger.exception('Import failed: %s', job_id)
            raise
