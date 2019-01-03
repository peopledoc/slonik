import pytest

from slonik import Connection


@pytest.fixture()
def conn():
    conn_ = Connection.from_env()
    yield conn_
    # TODO: close the connection
