import csv
import io
import uuid

from celery import Celery
from celery.utils.log import get_task_logger

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


@app.task(name='ping')
def ping():
    return 'pong'


def _count_csv_rows(data: bytes) -> int:
    text = io.TextIOWrapper(
        io.BytesIO(data),
        encoding='utf-8-sig',
        errors='replace',
        newline='')
    reader = csv.reader(text)
    header = next(reader, None)
    if header is None:
        return 0
    return sum(1 for _ in reader)


def _set_job_status(db,
                    job,
                    status: JobStatus,
                    *,
                    error: str | None = None) -> None:
    job.status = status
    job.error = error
    db.commit()


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
            total = _count_csv_rows(data)

            job.total_rows = total
            job.processed_rows = total
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
