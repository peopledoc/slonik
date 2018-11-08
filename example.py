import os

from slonik import Connection


if __name__ == '__main__':
    pghost = os.environ.get('PGHOST', 'localhost')
    pgport = os.environ.get('PGPORT', '5432')
    pguser = os.environ.get('PGUSER', 'postgres')
    pgpass = os.environ.get('PGPASSWORD', 'postgres')
    pgdb = os.environ.get('PGDATABASE', 'postgres')
    pgopts = os.environ.get('PGOPTIONS', '')

    conn = Connection(f'postgresql://{pguser}:{pgpass}@{pghost}:{pgport}/{pgdb}?{pgopts}')

    result = conn.get_one('SELECT 42')
    print(result)

    for result in conn.query('SELECT generate_series(1, 10)'):
        print(result)
