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

    async def generate(n):
        async for row in conn.query("SELECT *, pg_sleep(1) FROM generate_series(1, $1)", n):
            print(row)
    start = time.time()
    values = await asyncio.gather(
        generate(3),
        generate(3),
        generate(2),
    )
    print(time.time() - start)

    start = time.time()
    await conn.execute('SELECT 1, pg_sleep(1)')
    print(time.time() - start)

    await conn.close()


asyncio.run(main())
