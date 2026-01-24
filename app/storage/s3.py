import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.core.config import settings

_NOT_FOUND = {'404', 'NoSuchBucket', 'NotFound'}
_IGNORE_CREATE = {'BucketAlreadyOwnedByYou', 'BucketAlreadyExists'}


def get_s3_client(*, public: bool = False):
    endpoint = settings.s3_endpoint_url
    if public and settings.s3_public_endpoint_url:
        endpoint = settings.s3_public_endpoint_url

    return boto3.client(
        's3',
        endpoint_url=endpoint,
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


def presing_get(
        object_key: str,
        *,
        expires_seconds: int = 3600,
        download_filename: str | None = None,) -> str:
    s3 = get_s3_client(public=True)
    params = {
        'Bucket': settings.s3_bucket,
        'Key': object_key,
    }

    if download_filename:
        params['ResponseContentDisposition'] = (
            f'attachment; filename="{download_filename}"'
        )

    return s3.generate_presigned_url(
        'get_object',
        Params=params,
        ExpiresIn=expires_seconds,
    )
