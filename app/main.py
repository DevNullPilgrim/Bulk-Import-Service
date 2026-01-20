import uuid
from typing import Callable

import boto3
from botocore.config import Config
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from redis import Redis
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import engine, get_db
from app.models.import_job import ImportJob, JobStatus
from app.storage.s3 import put_bytes
from worker.celery_app import app as celery_app

app = FastAPI(
    title='Bulk Import Service',
    version='0.1.0'
)


def check_db() -> None:
    with engine.connect() as conn:
        conn.execute(text('SELECT 1'))


def check_redis() -> None:
    redis = Redis.from_url(
        settings.redis_url,
        socket_connect_timeout=1,
        socket_timeout=1
    )
    if not redis.ping():
        raise RuntimeError('Redis ping failed')


def check_s3() -> None:
    s3 = boto3.client(
        's3',
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(signature_version='s3v4'),
    )
    s3.head_bucket(Bucket=settings.s3_bucket)


def _run_check(fn: Callable[[], None]) -> str:
    try:
        fn()
        return 'up'
    except Exception as error:
        return f'down: {type(error).__name__}: {error}'


@app.get('/health')
def health() -> dict[str, object]:
    checks: dict[str, Callable[[], None]] = {
        'db': check_db,
        'redis': check_redis,
        's3': check_s3,
    }
    out = {name: _run_check(fn) for name, fn in checks.items()}
    ok = all(status == 'up' for status in out.values())
    return {'ok': ok, **out}


@app.post('/imports')
async def create_import(file: UploadFile = File(...),
                        db: Session = Depends(get_db),) -> dict[str, object]:
    data = await file.read()
    filename = file.filename or 'upload.csv'

    s3_key = put_bytes(data, filename=filename)
    job = ImportJob(
        status=JobStatus.pending,
        filename=filename,
        s3_key=s3_key
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    celery_app.send_task('process_import', args=[str(job.id)])
    return {'id': str(job.id), 'status': job.status}


@app.get('/imports/{job_id}')
def get_import(job_id: str, db: Session = Depends(get_db)) -> dict[str, object]:
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=442, detail='Invalid job_id')

    job = db.get(ImportJob, job_uuid)
    if not job:
        raise HTTPException(status_code=404, detail='Import job not found')

    return {
        'id': str(job.id),
        'status': job.status,
        'filename': job.filename,
        's3_key': job.s3_key,
        'total_rows': job.total_rows,
        'processed_rows': job.processed_rows,
        'error': job.error,
        'created_at': job.created_at,
    }
