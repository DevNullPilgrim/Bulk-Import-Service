import uuid
from http import HTTPStatus

from fastapi import (
    APIRouter,
    Depends,
    File,
    Header,
    HTTPException,
    Query,
    Response,
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
from app.storage.s3 import presign_get, put_bytes

router = APIRouter(prefix='/imports', tags=['imports'])


@router.post('', status_code=HTTPStatus.CREATED)
def create_import(
    response: Response,
    mode: ImportMode = Query(ImportMode.insert_only),
    file: UploadFile = File(...),
    idempotency_key: str | None = Header(
        default=None,
        alias='Idempotency-Key'),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Создает задачу импорта CSV и ставит ее в очередб Selery.

    Контракт:
      - Требует заголовок Idempotency-key: повтрный запрос тем же ключем
        (для текущего user_id) возвращает то же import job (200),
        первый запрос создает новый (201).
      - Файл читается целиком в памяти и загружается в S3;
        worker обрабатывает асинхронно.
    """
    if not idempotency_key or not idempotency_key.strip():
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail='Idempotency-Key header required')
    idem = idempotency_key.strip()

    existing = db.execute(
        select(ImportJob).where(
            ImportJob.user_id == user.id,
            ImportJob.idempotency_key == idem,
        )
    ).scalar_one_or_none()

    if existing:
        response.status_code = HTTPStatus.OK
        return jsonable_encoder(job_to_dict(existing))

    data = file.file.read()
    if not data:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST,
                            detail='empty file')

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
        response.status_code = HTTPStatus.OK
        return jsonable_encoder(job_to_dict(existing))

    db.refresh(job)

    try:
        celery_client.send_task('process_import', args=[str(job.id)])
    except Exception as errors:
        job.status = JobStatus.failed
        job.error = f'enqueue_failed: {type(errors).__name__}: {errors}'
        db.commit()
        raise HTTPException(status_code=HTTPStatus.SERVICE_UNAVAILABLE,
                            detail='queue unavailable')

    return jsonable_encoder(job_to_dict(job))


@router.get('/{job_id}')
def get_import(job_id: uuid.UUID,
               user: User = Depends(get_current_user),
               db: Session = Depends(get_db)) -> dict:
    """Возвраащет состояние  import job.

    Поля: status/processed_rows/total_rows обновляются worker'ом.
    Доступ ограничен текущим пользователем (user_id).
    """
    job = db.get(ImportJob, job_id)
    if job is None or job.user_id != user.id:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='not found')
    return jsonable_encoder(job_to_dict(job=job))


@router.get('/{job_id}/errors')
def get_import_errors(job_id: uuid.UUID,
                      user: User = Depends(get_current_user),
                      db=Depends(get_db)) -> dict:
    """Возвращает сслыку на errors.csv (presigned URL).

    Возвращает:
    - HTTPStatus.CONFLICT (409), если job ещё не завершён или отчёт не готов,
    - HTTPStatus.NOT_FOUND (404), если отчёта нет,
    - HTTPStatus.OK (200) + url, если errors.csv загружен в S3.
    """
    job = db.get(ImportJob, job_id)
    if job is None or job.user_id != user.id:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='Not found.')

    if not job.error_report_object_key:
        if job.status in (JobStatus.pending, JobStatus.processing):
            raise HTTPException(status_code=HTTPStatus.CONFLICT,
                                detail='Not ready.')
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='Error report.')

    url = presign_get(
        job.error_report_object_key,
        expires_seconds=3600,
        download_filename=f'errors_{job.id}.csv'
    )
    return {'url': url}
