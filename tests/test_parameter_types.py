from slonik import SlonikException

import pytest


def test_int(conn):
    assert conn.get_value('SELECT $1::int', 42) == 42

    with pytest.raises(SlonikException) as e:
        conn.get_value('SELECT $1::int', '42')
    assert 'type conversion error' in str(e.value)


def test_float(conn):
    assert conn.get_value('SELECT $1::float', 42.4) == 42.4

    with pytest.raises(SlonikException) as e:
        conn.get_value('SELECT $1::float', '42.4')
    assert 'type conversion error' in str(e.value)


def test_text(conn):
    assert conn.get_value('SELECT $1::text', 'foo') == 'foo'

    with pytest.raises(SlonikException) as e:
        conn.get_value('SELECT $1::text', 42)
    assert 'type conversion error' in str(e.value)
