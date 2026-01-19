from typing import Callable

import boto3
from botocore.config import Config
from fastapi import FastAPI
from redis import Redis
from sqlalchemy import text

from app.core.config import settings
from app.db.session import engine

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
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(signature_version="s3v4"),
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
