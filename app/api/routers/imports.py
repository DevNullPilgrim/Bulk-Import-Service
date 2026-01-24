import uuid

from fastapi import (
    APIRouter,
    Depends,
    File,
    Header,
    HTTPException,
    Query,
    UploadFile,
)
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.api.routers.serializers import job_to_dict
from app.core.celery_client import celery_client
from app.db.session import get_db
from app.models.import_job import ImportJob, ImportMode, JobStatus
from app.models.user import User
from app.storage.s3 import presing_get, put_bytes

router = APIRouter(prefix='/imports', tags=['imports'])


@router.post('')
def create_import(
    mode: ImportMode = Query(ImportMode.insert_only),
    file: UploadFile = File(...),
    idempotency_key: str | None = Header(
        default=None,
        alias='Idempotency-Key'),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:

    if not idempotency_key or not idempotency_key.strip():
        raise HTTPException(
            status_code=400, detail='Idempotency-Key header required')
    idem = idempotency_key.strip()

    existing = db.execute(
        select(ImportJob).where(
            ImportJob.user_id == user.id,
            ImportJob.idempotency_key == idem,
        )
    ).scalar_one_or_none()

    if existing:
        return jsonable_encoder(job_to_dict(existing))

    data = file.file.read()
    if not data:
        raise HTTPException(status_code=400, detail='empty file')

    filename = file.filename or 'upload.csv'
    s3_key = put_bytes(data, filename=filename)

    job = ImportJob(
        user_id=user.id,
        idempotency_key=idem,
        status=JobStatus.pending,
        mode=mode,
        filename=filename,
        s3_key=s3_key,
        total_rows=0,
        processed_rows=0,
        error=None,
        error_count=0,
        error_report_object_key=None,
    )

    db.add(job)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = db.execute(
            select(ImportJob).where(
                ImportJob.user_id == user.id,
                ImportJob.idempotency_key == idem,
            )
        ).scalar_one()
        return jsonable_encoder(job_to_dict(existing))

    db.refresh(job)
    celery_client.send_task('process_import', args=[str(job.id)])

    return jsonable_encoder(job_to_dict(job))


@router.get('/{job_id}')
def get_import(job_id: uuid.UUID,
               user: User = Depends(get_current_user),
               db: Session = Depends(get_db)) -> dict:
    job = db.get(ImportJob, job_id)
    if job is None or job.user_id != user.id:
        raise HTTPException(status_code=404, detail='not found')
    return jsonable_encoder(job_to_dict(job=job))


@router.get('/{job_id}/errors')
def get_import_errors(job_id: uuid.UUID,
                      user: User = Depends(get_current_user),
                      db=Depends(get_db)) -> dict:
    job = db.get(ImportJob, job_id)
    if job is None or job.user_id != user.id:
        raise HTTPException(status_code=404, detail='Not found.')

    if not job.error_report_object_key:
        if job.status in (JobStatus.pending, JobStatus.processing):
            raise HTTPException(status_code=409, detail='Not ready.')
        raise HTTPException(status_code=404, detail='Error report.')

    url = presing_get(
        job.error_report_object_key,
        expires_seconds=3600,
        download_filename=f'errors_{job.id}.csv'
    )
    return {'url': url}
