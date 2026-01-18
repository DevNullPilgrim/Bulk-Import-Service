from celery import Celery

from app.core.config import settings

app = Celery(
    'bulk_import',
    broker=settings.redis_url,
    backend=settings.redis_url,
)


@app.task(name='ping')
def ping():
    return 'pong'
