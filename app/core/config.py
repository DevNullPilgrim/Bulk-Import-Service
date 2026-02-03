from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    app_env: str = 'dev'
    database_url: str
    redis_url: str

    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str = 'imports'
    s3_region: str = 'us-east-1'
    s3_public_endpoint_url: str | None = None
    s3_presign_ttl_seconds: int = 3600

    jwt_secret: str
    jwt_alg: str = 'HS256'
    jwt_access_ttl_seconds: int = 3600

    batch_size: int = 500
    progress_every: int = 50
    import_slow_ms: int = 0

    max_upload_bytes: int = 50 * 1024 * 1024


settings = Settings()
