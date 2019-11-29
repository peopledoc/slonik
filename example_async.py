import asyncio
import time

from slonik import ConnectionPool


async def main():
    conn = ConnectionPool.from_env()
    start = time.time()
    values = await asyncio.gather(
        conn.get_one('SELECT 1, pg_sleep(1)'),
        conn.get_one('SELECT 2, pg_sleep(1)'),
        conn.get_one('SELECT 3, pg_sleep(1)'),
        conn.get_one('SELECT 4, pg_sleep(1)'),
    )
    print(time.time() - start)
    print(values)

    conn.close()


asyncio.run(main())
