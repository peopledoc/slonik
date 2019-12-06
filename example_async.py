import asyncio
import time

from slonik import AsyncConnection


async def main():
    conn = AsyncConnection.from_env()
    start = time.time()
    values = await asyncio.gather(
        conn.get_one('SELECT 1, pg_sleep(1)'),
        conn.get_one('SELECT 2, pg_sleep(1)'),
        conn.get_one('SELECT 3, pg_sleep(1)'),
        conn.get_one('SELECT 4, pg_sleep(1)'),
    )
    print(time.time() - start)
    print(values)

    #for row in await conn.query("SELECT *, pg_sleep(1) FROM generate_series(1, 10)"):
    #    print(row)
    async for row in conn.query("SELECT *, pg_sleep(1) FROM generate_series(1, 10)"):
        print(row)

    conn.close()


asyncio.run(main())
