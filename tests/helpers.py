import csv
import io
import uuid
from http import HTTPStatus

from .conftest import create_import, wait_job_done


def create_adn_wait(client,
                    *,
                    token: str,
                    idem_prefix: str,
                    mode: str,
                    scv_bytes: bytes,
                    timeout_s: float = 60.0):
    job = create_import(
        client,
        token=token,
        idem_key=f'{idem_prefix}-{uuid.uuid4().hex[:8]}',
        mode=mode,
        csv_bytes=scv_bytes,
    )
    final = wait_job_done(
        client,
        token=token,
        job_id=job['id'],
        timeout_s=timeout_s
    )
    return job, final


def download_errors_csv(client, url: str) -> tuple[list[str], list[dict]]:
    resp = client.get(url)
    assert resp.status_code == HTTPStatus.OK, resp.text

    text_csv = resp.content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text_csv))
    fieldnames = list(reader.fieldnames) if reader.fieldnames else []
    rows = list(reader)
    return fieldnames, rows
