from slonik import Connection


if __name__ == '__main__':
    conn = Connection.from_env()

    result = conn.get_one('SELECT 42')
    print(repr(result))

    result = conn.get_one('SELECT 2147483647')
    print(repr(result))

    result = conn.get_one("SELECT 'foo bar'")
    print(repr(result))
    result = conn.get_one("SELECT 'foo bar'::text")
    print(repr(result))

    result = conn.get_one('SELECT 42.4')
    print(repr(result))
    result = conn.get_one('SELECT 42.4::float')
    print(repr(result))

    result = conn.get_one('''SELECT '{"a": []}'::json''')
    print(repr(result))

    result = conn.get_one("SELECT '3d9d291d-8668-480f-98bf-46ee10d07a5d'::uuid")
    print(repr(result))

    for result in conn.query('SELECT generate_series(1, 10)'):
        print(repr(result))
