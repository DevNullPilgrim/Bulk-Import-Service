import uuid

import boto3
from botocore.config import Config

from app.core.config import settings


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(signature_version='s3v4'),
    )


def put_bytes(data: bytes, *, filename: str) -> str:
    safe_name = (filename or 'upload.csv').replace('/', '_').replace('\\', '_')
    key = f'imports/{uuid.uuid4()}_{safe_name}'
    s3 = get_s3_client()
    s3.put_object(Bucket=settings.s3_bucket,
                  Key=key,
                  Body=data)
    return key


def get_bytes(key: str) -> bytes:
    s3 = get_s3_client()
    obj = s3.get_object(
        Bucket=settings.s3_bucket,
        Key=key,)
    return obj['Body'].read()
