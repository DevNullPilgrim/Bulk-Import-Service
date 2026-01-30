import csv
import io
import uuid
from http import HTTPStatus

from .conftest import (
    create_import,
    get_errors_url,
    make_csv_bytes,
    rand_email,
    wait_job_done,
)


def create_and_wait(client,
                    *,
                    token: str,
                    idem_prefix: str,
                    mode: str,
                    csv_bytes: bytes,
                    timeout_s: float = 60.0):
    job = create_import(
        client,
        token=token,
        idem_key=f'{idem_prefix}-{uuid.uuid4().hex[:8]}',
        mode=mode,
        csv_bytes=csv_bytes,
    )
    final = wait_job_done(
        client,
        token=token,
        job_id=job['id'],
        timeout_s=timeout_s
    )
    return final


def download_errors_csv(client, url: str, host_header: str | None = None):
    headers = {}
    if host_header:
        headers["Host"] = host_header
    resp = client.get(url, headers=headers or None)
    assert resp.status_code == HTTPStatus.OK, resp.text

    text_csv = resp.content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text_csv))
    fieldnames = list(reader.fieldnames) if reader.fieldnames else []
    rows = list(reader)
    return fieldnames, rows


def seed_customer(client, user, *, email: str | None = None) -> str:
    """Создает одного customer через insert_only и ждет done. -> email."""
    email = email or rand_email("dup")
    csv1 = make_csv_bytes([[email, "A", "", "", "OldCity"]])
    final = create_and_wait(
        client,
        token=user.token,
        idem_prefix="seed",
        mode="insert_only",
        csv_bytes=csv1,
    )
    assert final["status"] == "done", final
    return email


def make_failed_job_with_errors(client,
                                user,
                                *,
                                dup_email: str) -> tuple[dict, str]:
    """Создаёт job, который должен упасть (дубль + невалидный email).

    Возвращает (final, errors_url).
    """
    csv_2 = make_csv_bytes([
        [dup_email, "B", "", "", "NewCity"],     # дубль в БД
        ["bad_email", "X", "", "", "Nowhere"],   # невалидный email
    ])

    job = create_import(
        client,
        token=user.token,
        idem_key="fail-" + uuid.uuid4().hex[:8],
        mode="insert_only",
        csv_bytes=csv_2,
    )

    final = wait_job_done(
        client=client,
        token=user.token,
        job_id=job["id"],
        timeout_s=60,
    )
    assert final["status"] == "failed", final

    errors_url = get_errors_url(
        client=client, token=user.token, job_id=job["id"])
    assert errors_url is not None
    return final, errors_url


def create_two_imports_with_same_idem(client,
                                      *,
                                      token1: str,
                                      token2: str,
                                      idem: str, csv_bytes: bytes):
    job_1 = create_import(
        client=client,
        token=token1,
        idem_key=idem,
        mode="insert_only",
        csv_bytes=csv_bytes,
    )
    job_2 = create_import(
        client=client,
        token=token2,
        idem_key=idem,
        mode="insert_only",
        csv_bytes=csv_bytes,
    )
    return job_1, job_2
