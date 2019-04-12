from slonik import Connection


def test_from_env(conn):
    conn = Connection.from_env(pguser='this-is', pgpassword='a-test')
    assert conn.dsn.startswith('postgresql://this-is:a-test@')


def test_get_value(conn):
    assert conn.get_value('SELECT 42') == 42
    assert conn.get_value('SELECT * FROM generate_series(1, 10)') == 1
    assert conn.get_value('SELECT $1::int', 42) == 42


def test_get_one(conn):
    assert conn.get_one('SELECT 42') == (42,)
    assert conn.get_one('SELECT 42, 21') == (42, 21)

    assert conn.get_one('SELECT * FROM generate_series(1, 10)') == (1,)
    assert conn.get_one("SELECT 'foo', generate_series(1, 10)") == ('foo', 1)

    assert conn.get_one('SELECT $1::int, $2::text', 42, 'foo') == (42, 'foo')


def test_query(conn):
    assert list(conn.query('SELECT 42')) == [(42,)]
    assert list(conn.query('SELECT 42, 21')) == [(42, 21)]

    result = list(conn.query('SELECT * FROM generate_series(1, 10)'))
    assert result == [(i,) for i in range(1, 11)]

    result = list(conn.query("SELECT 'foo', generate_series(1, 10)"))
    assert result == [('foo', i) for i in range(1, 11)]

    result = list(conn.query(
        'SELECT * FROM generate_series($1::int, $2::int)', 1, 10,
    ))
    assert result == [(i,) for i in range(1, 11)]


def test_execute(conn):
    conn.execute('DROP TABLE IF exists test_table')
    conn.execute('CREATE TABLE test_table(name text, value int)')

    conn.execute("INSERT INTO test_table(name, value) VALUES ('foo', 42)")
    conn.execute(
        'INSERT INTO test_table(name, value) VALUES ($1, $2)',
        'bar', 21,
    )
    result = list(conn.query("SELECT * FROM test_table ORDER BY name ASC"))
    assert result == [('bar', 21), ('foo', 42)]
