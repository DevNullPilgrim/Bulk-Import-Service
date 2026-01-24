from celery import Celery

from app.core.config import settings


def make_celery_client() -> Celery:
    return Celery(
        'bulk_import',
        broker=settings.redis_url,
        backend=settings.redis_url,)


celery_client = make_celery_client()
