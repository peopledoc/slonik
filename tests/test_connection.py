from slonik import Connection


def test_from_env(conn):
    conn = Connection.from_env(pguser='this-is', pgpassword='a-test')
    assert conn.dsn.startswith('postgresql://this-is:a-test@')


def test_get_one(conn):
    assert conn.get_one('SELECT 42') == 42


def test_query(conn):
    assert list(conn.query('SELECT 42')) == [42]

    result = list(conn.query('SELECT * FROM generate_series(1, 10)'))
    assert result == list(range(1, 11))
