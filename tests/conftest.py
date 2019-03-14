import pytest

from slonik import Connection


@pytest.fixture()
def conn():
    conn_ = Connection.from_env()
    conn_.execute('BEGIN')
    try:
        yield conn_
    finally:
        conn_.execute('ROLLBACK')
        # TODO: close the connection
