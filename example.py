from slonik import Connection
from slonik import SlonikException


if __name__ == '__main__':
    conn = Connection.from_env()

    result = conn.get_value('SELECT 42')
    print(repr(result))

    result = conn.get_value('SELECT 2147483647')
    print(repr(result))

    result = conn.get_value("SELECT 'foo bar'")
    print(repr(result))
    result = conn.get_value("SELECT 'foo bar'::text")
    print(repr(result))

    result = conn.get_value('SELECT 42.4')
    print(repr(result))
    result = conn.get_value('SELECT 42.4::float')
    print(repr(result))

    result = conn.get_value('''SELECT '{"a": []}'::json''')
    print(repr(result))

    result = conn.get_value("SELECT '3d9d291d-8668-480f-98bf-46ee10d07a5d'::uuid")
    print(repr(result))

    result = conn.get_one("SELECT 42, 'foo bar'::text")
    print(repr(result))

    for result in conn.query('SELECT generate_series(1, 10)'):
        print(repr(result))

    result = conn.get_value('SELECT $1::int+$2', 42, 8)
    print(repr(result))

    result = conn.get_value('SELECT $1::text', 'abc')
    print(repr(result))

    result = conn.get_value('SELECT $1::float', 42.4)
    print(repr(result))

    #####

    conn.execute('BEGIN')
    conn.execute('DROP TABLE IF exists toto')
    conn.execute('CREATE TABLE toto(value text, sum int)')
    print(list(conn.query('SELECT * FROM toto')))
    conn.execute('INSERT INTO toto(value, sum) VALUES ($1, $2)', 'salut', 15)
    conn.execute('INSERT INTO toto(value, sum) VALUES ($1, $2)', 'oki', 32)
    conn.execute('INSERT INTO toto(value) VALUES ($1)', 'pouet')
    print(list(conn.query('SELECT * FROM toto')))
    conn.execute('ROLLBACK')

    #####

    try:
        conn.get_value('SELECT bar FROM foo')
    except SlonikException as e:
        print(e)

    try:
        conn.execute('INSERT INTO foo(bar) VALUES (0)')
    except SlonikException as e:
        print(e)

    #####

    conn.close()
