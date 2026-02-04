import csv
import io
import time
import uuid
from typing import Iterator

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

app.conf.broker_connection_retry_on_startup = True
app.conf.broker_connection_retry = True

logger = get_task_logger(__name__)

PROGRESS_EVERY = settings.progress_every
BATCH_SIZE = settings.batch_size
IMPORT_SLOW_MS = settings.import_slow_ms


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


def parse_customer_row(row: list[str],
                       row_num: int) -> tuple[dict | None, str | None]:
    if not row:
        return None, f'row {row_num}: empty row'

    email = _norm(row[0])
    if not email:
        return None, f'row {row_num}: empty email'

    if '@' not in email or '.' not in email.split('@')[-1]:
        return None, f'row {row_num}: invalid email "{email}"'

    return {
        'id': uuid.uuid4(),
        'email': email,
        'first_name': _norm(row[1]) if len(row) > 1 else None,
        'last_name': _norm(row[2]) if len(row) > 2 else None,
        'phone': _norm(row[3]) if len(row) > 3 else None,
        'city': _norm(row[4]) if len(row) > 4 else None,
    }, None


class BatchBuffer:
    """Буфер строк для пакетной записи в БД.

    rows: хранит подготовленные payload'ы для INSERT/UPSRT,
    rows_nums: хранит номер строк исходного CSV (для errors.csv).
    """

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


class UpsertFlusher:
    """Запись в режиме upsert.

    Реализован через PostgreSQL INSERT ... ON CONFLICT(email) DO UPDATE.
    Дубли в файле могут считаться ошибками.
    """

    def flush(
        self,
        db,
        buffer: BatchBuffer,
        errors: list[str],
        error_rows: list[ErrorRow],
    ) -> None:
        if not buffer.rows:
            return

        stmt = pg_insert(Customer).values(buffer.rows)

        # обновляем поля, но НЕ трогаем id/email
        stmt = stmt.on_conflict_do_update(
            index_elements=[Customer.email],
            set_={
                'first_name': stmt.excluded.first_name,
                'last_name': stmt.excluded.last_name,
                'phone': stmt.excluded.phone,
                'city': stmt.excluded.city,
            },
        )

        db.execute(stmt)
        db.commit()


class InsertOnlyFlusher:
    """Запись в режиме Insert_only.

    Правило: если email уже есть в БД или повторяется в самом файле:
      строка попадает в errors, остальные строки вставляются.
    """

    def flush(
        self,
        db,
        buffer: BatchBuffer,
        errors: list[str],
        error_rows: list[ErrorRow],
    ) -> None:
        if not buffer.rows:
            return

        emails = [row['email'] for row in buffer.rows]
        existing = set(
            db.execute(
                select(Customer.email).where(Customer.email.in_(emails))
            )
            .scalars()
            .all()
        )

        to_insert: list[dict] = []

        for payload, rn in zip(buffer.rows, buffer.row_nums):
            email = payload['email']

            if email in existing:
                msg = f'email already exists "{email}"'

                if len(errors) < 3:
                    errors.append(f'row {rn}: {msg}')

                raw = ','.join(
                    [
                        payload.get('email') or '',
                        payload.get('first_name') or '',
                        payload.get('last_name') or '',
                        payload.get('phone') or '',
                        payload.get('city') or '',
                    ]
                )
                error_rows.append(ErrorRow(row=rn, error=msg, raw=raw))
            else:
                to_insert.append(payload)

        if to_insert:
            db.execute(pg_insert(Customer).values(to_insert))

        db.commit()


def get_flusher(mode: ImportMode):
    return (
        InsertOnlyFlusher()
        if mode == ImportMode.insert_only else UpsertFlusher())


def _short_error_summary(errors_head: list[str],
                         total: int,
                         limit: int = 3) -> str:
    if total == 0:
        return ''
    head = ' | '.join(errors_head[:limit])
    if total > limit:
        return f'errors: {total}; first: {head} ...'
    return f'errors: {total}; first: {head}'


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
                flusher) -> tuple[int, list[str], list[ErrorRow], int]:
    processed = 0
    errors: list[str] = []
    error_rows: list[ErrorRow] = []
    error_count = 0
    seen_emails: set[str] = set()
    buffer = BatchBuffer(BATCH_SIZE)

    for row_num, row in enumerate(iter_csv_rows(data), start=1):
        processed += 1
        payload, err = parse_customer_row(row, row_num)

        if err:
            error_count += 1

            if len(errors) < 3:
                errors.append(err)

            error_rows.append(ErrorRow(row=row_num,
                                       error=err,
                                       raw=','.join(row)))
        else:
            email = payload['email']
            if email in seen_emails:
                msg = f'duplicate email "{email}" in file'
                error_count += 1

                if len(errors) < 3:
                    errors.append(f'row {row_num}: {msg}')
                error_rows.append(ErrorRow(row=row_num,
                                           error=msg,
                                           raw=','.join(row)))
            else:
                seen_emails.add(email)
                buffer.add(payload, row_num)

        if IMPORT_SLOW_MS:
            time.sleep(IMPORT_SLOW_MS / 1000)

        if processed % PROGRESS_EVERY == 0:
            _update_job(db, job_uuid, processed_rows=processed)

        if buffer.full():
            flusher.flush(db, buffer, errors, error_rows)
            buffer.clear()

    flusher.flush(db, buffer, errors, error_rows)
    buffer.clear()

    error_count = len(error_rows)
    return processed, errors, error_rows, error_count


def run_import(db, job_uuid: uuid.UUID, s3_key: str, mode: ImportMode) -> None:
    _update_job(db, job_uuid, status=JobStatus.processing,
                error=None, processed_rows=0)

    data = get_bytes(s3_key)
    total = count_csv_rows(data=data)
    _update_job(db, job_uuid, total_rows=total, processed_rows=0)

    flusher = get_flusher(mode)
    processed, errors, error_rows, error_count = process_csv(
        db, job_uuid, data, flusher)

    report_key = None
    if error_rows:
        errors_bytes = build_errors_csv(error_rows)
        report_key = put_bytes(errors_bytes, filename=f'errors_{job_uuid}.csv')

    final_status = JobStatus.done if error_count == 0 else JobStatus.failed
    final_error = None if error_count == 0 else _short_error_summary(
        errors, total=error_count)

    _update_job(
        db,
        job_uuid,
        processed_rows=processed,
        status=final_status,
        error=final_error,
        error_report_object_key=report_key,
        error_count=error_count,
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
