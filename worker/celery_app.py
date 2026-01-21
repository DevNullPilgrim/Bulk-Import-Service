import csv
import time
import io
import os
import uuid
from typing import Iterable, Iterator

from celery import Celery
from celery.utils.log import get_task_logger
from sqlalchemy import select, update

from app.core.config import settings
from app.db.session import SessionLocal
from app.models.import_job import ImportJob, JobStatus
from app.storage.s3 import get_bytes

app = Celery(
    'bulk_import',
    broker=settings.redis_url,
    backend=settings.redis_url,
)

logger = get_task_logger(__name__)

# обновлять processed_rows каждые N строк
PROGRESS_EVERY = 50
SLOW_MS = int(os.getenv("IMPORT_SLOW_MS", "0"))


@app.task(name='ping')
def ping():
    return 'pong'


def _csv_reader(data: bytes) -> Iterator[list[str]]:
    text = io.TextIOWrapper(
        io.BytesIO(data),
        encoding='utf-8-sig',
        errors='replace',
        newline='')
    reader = csv.reader(text)
    next(reader, None)
    return reader


def count_csv_rows(data: bytes) -> int:
    return sum(1 for _ in _csv_reader(data))


def iter_csv_rows(data: bytes) -> Iterable[list[str]]:
    yield from _csv_reader(data)


def _set_job_status(db,
                    job,
                    status: JobStatus,
                    *,
                    error: str | None = None) -> None:
    job.status = status
    job.error = error
    db.commit()


def _update_job(db, job_uuid: uuid.UUID, **fields) -> None:
    db.execute(
        update(ImportJob)
        .where(ImportJob.id == job_uuid)
        .values(**fields)
    )
    db.commit()


def _get_s3_key(db, job_uuid: uuid.UUID) -> None:
    return db.scalar(
        select(ImportJob.s3_key)
        .where(ImportJob.id == job_uuid)
    )


@app.task(name='process_import', bind=True)
def process_import(self, job_id: str) -> str:
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        logger.error('Invalid job id: %s', job_id)
        return 'bad_id'

    with SessionLocal() as db:
        job = db.get(ImportJob, job_uuid)

        if job is None:
            logger.error('ImportJob not found: %s', job_id)
            return 'not_found'

        try:
            _set_job_status(db, job, JobStatus.processing, error=None)

            data = get_bytes(job.s3_key)
            total = count_csv_rows(data)

            job.total_rows = total
            job.processed_rows = total

            # db.commit()
            # processed = 0
            # for _row in iter_csv_rows(data):
            #     processed += 1

            #     # ВОТ ТУТ КОНКРЕТНО замедление
            #     if SLOW_MS:
            #         time.sleep(SLOW_MS / 1000)

            #     # периодически пишем прогресс
            #     if processed % PROGRESS_EVERY == 0:
            #         job.processed_rows = processed
            #         db.commit()
            # job.processed_rows = processed
            _set_job_status(db, job, JobStatus.done, error=None)
            return 'ok'

        except Exception as e:
            db.rollback()
            try:
                job.status = JobStatus.failed
                job.error = f'{type(e).__name__}: {e}'
                db.commit()
            except Exception:
                db.rollback()
            logger.exception('Import failed: %s', job_id)
            raise
