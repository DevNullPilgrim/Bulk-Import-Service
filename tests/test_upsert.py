import uuid

from sqlalchemy import text

from .conftest import create_import, make_csv_bytes, wait_job_done


def test_upsert_updates_existing_customer(client, user, db_engine):
    email = f'up_{uuid.uuid4().hex}@test.com'

    csv1 = make_csv_bytes([[email, "Old", "", "", "OldCity"]])
    j1 = create_import(
        client,
        token=user.token,
        idem_key="up1-" + uuid.uuid4().hex[:8],
        mode="insert_only",
        csv_bytes=csv1,
    )
    f1 = wait_job_done(client, token=user.token, job_id=j1["id"], timeout_s=60)
    assert f1["status"] == "done"

    csv2 = make_csv_bytes([[email, "New", "", "", "NewCity"]])
    j2 = create_import(
        client,
        token=user.token,
        idem_key="up2-" + uuid.uuid4().hex[:8],
        mode="upsert",
        csv_bytes=csv2,
    )
    f2 = wait_job_done(client, token=user.token, job_id=j2["id"], timeout_s=60)
    assert f2["status"] == "done"

    with db_engine.connect() as conn:
        row = conn.execute(
            text("SELECT first_name, city FROM customers WHERE email=:email"),
            {"email": email},
        ).one()
        assert row[0] == "New"
        assert row[1] == "NewCity"

        cnt = conn.execute(
            text("SELECT count(*) FROM customers;")).scalar_one()
        assert cnt == 1
