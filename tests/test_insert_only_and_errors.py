
from sqlalchemy import text

from .conftest import make_csv_bytes, rewrite_presigned_for_container
from .helpers import (
    create_and_wait,
    download_errors_csv,
    make_failed_job_with_errors,
    seed_customer,
)


def test_insert_only_creates_customers(client, user, db_engine):
    email = seed_customer(client=client, user=user)
    with db_engine.connect() as conn:
        cnt = conn.execute(
            text("SELECT count(*) FROM customers WHERE email=:email"),
            {"email": email}).scalar_one()
    assert cnt == 1


def test_insert_only_duplicate_fails(client, user):
    email = seed_customer(client=client, user=user)
    final, _ = make_failed_job_with_errors(client, user, dup_email=email)
    assert final["status"] == "failed"


def test_failed_job_has_errors_url(client, user):
    email = seed_customer(client=client, user=user)
    _, url = make_failed_job_with_errors(client, user, dup_email=email)
    assert url is not None


def test_errors_csv_format(client, user):
    email = seed_customer(client=client, user=user)
    _, errors_url = make_failed_job_with_errors(client, user, dup_email=email)

    download_url, host_header = rewrite_presigned_for_container(errors_url)
    headers, rows = download_errors_csv(
        client, download_url, host_header=host_header)

    assert headers == ["row", "error", "raw"]
    assert len(rows) >= 1


def test_insert_only_does_not_create_duplicates(client, user, db_engine):
    email = seed_customer(client=client, user=user)

    csv_dup = make_csv_bytes([[email, "A", "", "", "OldCity"]])
    final = create_and_wait(
        client,
        token=user.token,
        idem_prefix="dup",
        mode="insert_only",
        csv_bytes=csv_dup,
    )
    assert final["status"] == "failed"

    with db_engine.connect() as conn:
        cnt = conn.execute(
            text("SELECT count(*) FROM customers WHERE email=:email"),
            {"email": email},
        ).scalar_one()
    assert cnt == 1
