import uuid
from http import HTTPStatus

from .conftest import (
    auth_headers,
    create_import,
    make_csv_bytes,
    rand_email,
    wait_job_done,
)
from .helpers import create_two_imports_with_same_idem


def test_idempotency_returns_same_job_id_for_same_user(client, user):
    csv_bytes = make_csv_bytes([[rand_email('a'), 'A', '', '', 'Calgary']])
    idem = 'K1-' + uuid.uuid4().hex[:8]

    job_1, job_2 = create_two_imports_with_same_idem(
        client,
        token1=user.token,
        token2=user.token,
        idem=idem,
        csv_bytes=csv_bytes
    )

    assert job_1['id'] == job_2['id']


def test_idempotency_is_scoped_to_user(client, user, other_user):
    csv_bytes = make_csv_bytes([[rand_email('sc'), 'A', '', '', 'Calgary']])
    idem = 'SAME-' + uuid.uuid4().hex[:8]

    job_1, job_2 = create_two_imports_with_same_idem(
        client,
        token1=user.token,
        token2=other_user.token,
        idem=idem, csv_bytes=csv_bytes
    )

    assert job_1['id'] != job_2['id']


def test_import_completes_and_counts(client, user):
    csv_bytes = make_csv_bytes([
        [rand_email('a'), 'A', 'AA', '', 'Calgary'],
        [rand_email('b'), 'B', 'BB', '', 'Calgary'],
    ])
    job = create_import(client=client,
                        token=user.token,
                        idem_key='k2-'+uuid.uuid4().hex[:8],
                        mode='insert_only', csv_bytes=csv_bytes)
    final = wait_job_done(client=client,
                          token=user.token,
                          job_id=job['id'],
                          timeout_s=30.0)
    if final['status'] != 'done':
        raise AssertionError(f'Import failed: {final}')
    assert final['total_rows'] == 2
    assert final['processed_rows'] == 2


def test_job_is_scoped_to_user(client, user, other_user):
    csv_bytes = make_csv_bytes([['a@test.com', 'A', '', '', 'X']])
    job = create_import(client,
                        token=user.token,
                        idem_key='k2-'+uuid.uuid4().hex[:8],
                        mode='insert_only', csv_bytes=csv_bytes)

    read = client.get(f'/imports/{job["id"]}',
                      headers=auth_headers(other_user.token))
    assert read.status_code in (
        HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND), read.text


def test_missing_idempotency_key_returns_400(client, user):
    csv_bytes = make_csv_bytes([[rand_email("x"), "X", "", "", "City"]])
    files = {"file": ("customer.csv", csv_bytes, "text/csv")}

    resp = client.post(
        "/imports",
        params={"mode": "insert_only"},
        headers=auth_headers(user.token),  # без idem_key
        files=files,
    )
    assert resp.status_code == HTTPStatus.BAD_REQUEST, resp.text
