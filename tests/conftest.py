"""Фикстуры и хелперы для интерграционных тестов Bulk Import Service.

Предположения:
    - API доступен по TEST_BASE_URL (по умолчанию:http://localhost:8000)
    - /imports/{id}/errors возвращает presigned URL на MinIO
        (часто http://localhost:9000/...).
Важно:
    - Тест запускается в нутри контейнера Docker,presigned URL с localhost:9000
        нужно переписать на minio:9000
        (внутри контейнера localhost указывает на сам контейнер).
    - перед каждым иестом таблица в БД очищается TRUNCATE (ТОЛЬКО DEV-стек).
"""

import csv
import io
import os
import time
import uuid
from dataclasses import dataclass
from http import HTTPStatus
from typing import Generator
from urllib.parse import urlparse, urlunparse

import httpx
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# def _in_docker() -> bool:
#     return os.path.exists('/.dockerenv')


def rewrite_presigned_for_container(url: str) -> tuple[str, str | None]:
    """API отдаёт presigned URL под localhost:9000 (для браузера на хосте).

        Внутри контейнера localhost = контейнер, поэтому:
        - реально идём на host.docker.internal:9000
        - но Host оставляем localhost:9000, чтобы подпись (SigV4) совпала.
    """
    parsed = urlparse(url)
    if parsed.hostname in ("localhost", "127.0.0.1") and (parsed.port == 9000 or parsed.port is None):
        new_url = urlunparse(parsed._replace(
            netloc="host.docker.internal:9000"))
        return new_url, "localhost:9000"
    return url, None


def _base_url() -> str:
    return os.getenv('TEST_BASE_URL', 'http://localhost:8000').rstrip('/')


@pytest.fixture(scope='session')
def client() -> Generator[httpx.Client, None, None]:
    client = httpx.Client(base_url=_base_url(),
                          timeout=httpx.Timeout(30.0),
                          follow_redirects=True)
    try:
        yield client
    finally:
        client.close()


@dataclass(frozen=True)
class UserCreds:
    email: str
    password: str
    token: str


def _register_and_token(client: httpx.Client,
                        *,
                        email: str,
                        password: str) -> str:
    register = client.post('/auth/register',
                           json={'email': email, 'password': password})
    assert register.status_code in (HTTPStatus.OK,
                                    HTTPStatus.CREATED,
                                    HTTPStatus.CONFLICT), register.text

    token_resp = client.post('/auth/token',
                             json={'email': email, 'password': password})
    assert token_resp.status_code == HTTPStatus.OK, token_resp.text

    data = token_resp.json()
    assert 'access_token' in data, data
    return data['access_token']


def _make_user(client: httpx.Client) -> UserCreds:
    email = f'u_{uuid.uuid4().hex[:10]}@test.com'
    password = 'pass12345'
    token = _register_and_token(client, email=email, password=password)
    return UserCreds(email=email, password=password, token=token)


@pytest.fixture()
def user(client: httpx.Client) -> UserCreds:
    return _make_user(client)


@pytest.fixture()
def other_user(client: httpx.Client) -> UserCreds:
    return _make_user(client)


def auth_headers(token: str,
                 *,
                 idem_key: str | None = None) -> dict[str, str]:
    headers = {'Authorization': f'Bearer {token}'}
    if idem_key is not None:
        headers['Idempotency-Key'] = idem_key
    return headers


def make_csv_bytes(rows: list[list[str]]) -> bytes:
    out = io.StringIO(newline='')
    writer = csv.writer(out)
    writer.writerow(['email', 'first_name', 'last_name', 'phone', 'city'])
    for row in rows:
        writer.writerow(row)
    return out.getvalue().encode('utf-8')


def create_import(client: httpx.Client,
                  *,
                  token: str,
                  idem_key: str,
                  mode: str,
                  csv_bytes: bytes,
                  filename: str = 'customer.csv',) -> dict:
    files = {'file': (filename, csv_bytes, 'text/csv')}
    read = client.post(
        '/imports',
        params={'mode': mode},
        headers=auth_headers(token, idem_key=idem_key),
        files=files,
    )
    assert read.status_code in (HTTPStatus.OK, HTTPStatus.CREATED), read.text
    data = read.json()
    assert 'id' in data, data
    return data


def get_import(client: httpx.Client,
               *,
               token: str,
               job_id: str) -> dict:
    read = client.get(f'/imports/{job_id}', headers=auth_headers(token=token))
    return {'status_code': read.status_code,
            'json': (read.json() if read.headers.get('content-type', '')
                     .startswith('application/json') else None),
            'text': read.text}


def wait_job_done(client: httpx.Client,
                  *,
                  token: str,
                  job_id: str,
                  timeout_s: float = 30.0,
                  poll_s: float = 0.5,) -> dict:
    """Ожидает завершене job, опрашивая GET/imports/{id} до done/failed.

    Делает polling с интервалом poll_s до timeout_s. Если за timeout job не
        перешёл в done/failed — падает с AssertionError и последним ответом.
    """
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        read = client.get(f'/imports/{job_id}',
                          headers=auth_headers(token=token))
        if read.status_code != HTTPStatus.OK:
            last = (read.status_code, read.text)
            time.sleep(poll_s)
            continue
        data = read.json()
        status = data.get('status')

        if status in ('done', 'failed'):
            return data
        last = data
        time.sleep(poll_s)
    raise AssertionError(f'Job not finished in {timeout_s}s; last={last}')


def get_errors_url(client: httpx.Client,
                   token: str,
                   job_id: str) -> str | None:
    read = client.get(f'/imports/{job_id}/errors',
                      headers=auth_headers(token=token))
    if read.status_code == HTTPStatus.NOT_FOUND:
        return None
    assert read.status_code == HTTPStatus.OK, read.text
    return read.json().get("url")


@pytest.fixture(scope='session')
def db_url() -> str | None:
    try:
        from app.core.config import settings
        return settings.database_url
    except Exception:
        return os.getenv('DATABASE_URL')


@pytest.fixture(scope='session')
def db_engine(db_url: str | None) -> Generator[Engine, None, None]:
    if not db_url:
        pytest.skip(
            'No DATABASE_URL / settings.database_url, skipping DB assertions')
    engine = create_engine(db_url, future=True, pool_pre_ping=True)
    try:
        with engine.connect() as connect:
            connect.exec_driver_sql('SELECT 1')
        yield engine
    except Exception as errors:
        pytest.skip(f'Cannot connect to DB: {errors}, skipping DB assertions')
    finally:
        engine.dispose()


@pytest.fixture(autouse=True)
def clean_db(db_engine):
    """Чистим БД перед каждым тестом (DEV ONLY).

    TRUNCATE import_jobs/customers/users с RESTART IDENTITY CASCADE.
    ВАЖНО: гоняй это на dev-стеке, иначе снесёшь реальные данные.
    """
    with db_engine.begin() as conn:
        conn.execute(text(
            "TRUNCATE TABLE import_jobs, customers, users RESTART IDENTITY CASCADE"
        ))
    yield


def rand_email(prefix="c") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}@test.com"
