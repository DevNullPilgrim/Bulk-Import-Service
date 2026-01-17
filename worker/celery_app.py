from celery import Celery

from app.core.config import Settings

celery = Celery(
    'bulk_import',
    broker=Settings.redis_url,
    backend=Settings.redis_url,
)


@celery.task(name='ping')
def ping():
    return 'pong'
