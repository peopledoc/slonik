from slonik import Connection
from slonik import SlonikException

import pytest


def test_connection_error():
    conn = Connection('postgresql://255.255.255.255/db')
    with pytest.raises(SlonikException) as e:
        conn._conn
    assert 'Network is unreachable' in str(e.value)


def test_query_error(conn):
    with pytest.raises(SlonikException) as e:
        list(conn.query('SELECT bar FROM foo'))
    assert 'relation "foo" does not exist' in str(e.value)

    with pytest.raises(SlonikException) as e:
        list(conn.query('SELECT bar MORF foo'))
    assert 'syntax error at or near "foo"' in str(e.value)


def test_execute_error(conn):
    with pytest.raises(SlonikException) as e:
        conn.execute('DELETE FROM foo')
    assert 'relation "foo" does not exist' in str(e.value)

    with pytest.raises(SlonikException) as e:
        conn.execute('DELETE MORF foo')
    assert 'syntax error at or near "MORF"' in str(e.value)


def test_param_type_error(conn):
    with pytest.raises(SlonikException) as e:
        conn.get_value('SELECT $1::text', 42)
    assert str(e.value) == ("type conversion error: cannot convert to or from "
                            "a Postgres value of type `text`")
