import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.core.config import settings

_NOT_FOUND = {'404', 'NoSuchBucket', 'NotFound'}
_IGNORE_CREATE = {'BucketAlreadyOwnedByYou', 'BucketAlreadyExists'}


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(signature_version='s3v4'),
    )


def _error_code(error: ClientError) -> str:
    return str(error.response.get('Error', {}).get('code', ''))


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        if _error_code(e) not in _NOT_FOUND:
            raise
    try:
        s3.create_bucket(Bucket=bucket)
    except ClientError as e:
        if _error_code(e) not in _IGNORE_CREATE:
            raise


def put_bytes(data: bytes, *, filename: str) -> str:
    safe_name = (filename or 'upload.csv').replace('/', '_').replace('\\', '_')
    key = f'imports/{uuid.uuid4()}_{safe_name}'

    s3 = get_s3_client()
    ensure_bucket(s3, settings.s3_bucket)

    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=data,
    )

    return key


def get_bytes(key: str) -> bytes:
    s3 = get_s3_client()
    ensure_bucket(s3, settings.s3_bucket)

    obj = s3.get_object(
        Bucket=settings.s3_bucket,
        Key=key,
    )
    return obj['Body'].read()
