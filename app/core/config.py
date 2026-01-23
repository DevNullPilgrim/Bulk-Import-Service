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

    jwt_secret: str = 'my_super_secret'
    jwt_alg: str = 'HS256'
    jwt_access_ttl_seconds: int = 3600


settings = Settings()
