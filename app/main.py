import uuid

import sqlalchemy as sa
from celery import Celery
from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import get_db
from app.models.import_job import ImportJob, ImportMode, JobStatus
from app.storage.s3 import put_bytes, presign_get
from fastapi import HTTPException

celery_client = Celery(
    'bulk_import',
    broker=settings.redis_url,
    backend=settings.redis_url,
)

app = FastAPI(title='Bulk Import Service')


def _job_to_dict(job: ImportJob) -> dict:
    return {
        'id': str(job.id),
        'status': job.status.value if hasattr(job.status, 'value') else str(job.status),
        'mode': job.mode.value if hasattr(job.mode, 'value') else str(job.mode),
        'filename': job.filename,
        'total_rows': job.total_rows,
        'processed_rows': job.processed_rows,
        'error': job.error,
        'created_at': job.created_at.isoformat() if getattr(job, 'created_at', None) else None,
    }


@app.get('/health')
def health(db: Session = Depends(get_db)) -> dict:
    # DB check (самая частая причина "connection refused" при старте)
    try:
        db.execute(sa.text('SELECT 1'))
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f'db: {type(e).__name__}: {e}',
        )

    return {'status': 'ok'}


@app.post('/imports')
async def create_import(
    mode: ImportMode = Query(ImportMode.insert_only),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
) -> dict:
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail='empty file')

    filename = file.filename or 'upload.csv'
    s3_key = put_bytes(data, filename=filename)

    job = ImportJob(
        status=JobStatus.pending,
        filename=filename,
        s3_key=s3_key,
        total_rows=0,
        processed_rows=0,
        mode=mode,
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    celery_client.send_task('process_import', args=[str(job.id)])

    return jsonable_encoder(_job_to_dict(job))


@app.get('/imports/{job_id}')
def get_import(job_id: str, db: Session = Depends(get_db)) -> dict:
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=404, detail='not found')

    job = db.get(ImportJob, job_uuid)
    if job is None:
        raise HTTPException(status_code=404, detail='not found')

    return jsonable_encoder(_job_to_dict(job))


# (опционально) временный алиас, чтобы не ломать старые запросы
@app.get('/import/{job_id}', include_in_schema=False)
def get_import_alias(job_id: str, db: Session = Depends(get_db)) -> dict:
    return get_import(job_id, db)


@app.get("/imports/{job_id}/errors")
def get_import_errors(job_id: uuid.UUID, db=Depends(get_db)):
    job = db.get(ImportJob, job_id)
    if not job:
        raise HTTPException(404, "not found")

    if not job.error_report_object_key:
        # пока ещё работает — отчёта может не быть
        if job.status in (JobStatus.pending, JobStatus.processing):
            raise HTTPException(409, "not ready")
        raise HTTPException(404, "no errors report")

    url = presign_get(
        job.error_report_object_key,
        expires_seconds=3600,
        download_filename=f"errors_{job_id}.csv",
    )
    return {"url": url}
