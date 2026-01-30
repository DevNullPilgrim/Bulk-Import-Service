import uuid
from http import HTTPStatus

from .conftest import (
    create_import,
    make_csv_bytes,
    rand_email,
)


def test_post_imports_without_token_returns_401(client):
    csv_bytes = make_csv_bytes([[rand_email('a'), 'A', '', '', 'X']])
    files = {'file': ('customer.csv', csv_bytes, 'text/csv')}

    resp = client.post('/imports', params={'mode': 'insert_only'}, files=files)
    assert resp.status_code == HTTPStatus.UNAUTHORIZED, resp.text


def test_get_import_without_token_returns_401(client, user):
    csv_bytes = make_csv_bytes([[rand_email("b"), "B", "", "", "Y"]])
    job = create_import(client=client,
                        token=user.token,
                        idem_key='author-'+uuid.uuid4().hex[:8],
                        mode='insert_only',
                        csv_bytes=csv_bytes)
    resp = client.get(f"/imports/{job['id']}")
    assert resp.status_code == HTTPStatus.UNAUTHORIZED
