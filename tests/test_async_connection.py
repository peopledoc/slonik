import pytest

from slonik import AsyncConnection


pytestmark = pytest.mark.asyncio


@pytest.fixture()
async def conn():
    # Should use a global transaction
    conn = AsyncConnection.from_env()
    try:
        yield conn
    finally:
        await conn.close()


async def test_get_value(conn):
    assert await conn.get_value('SELECT 42') == 42


async def test_get_one(conn):
    assert await conn.get_one('SELECT 42, 21') == (42, 21)


async def test_query(conn):
    result = [
        r async for r in conn.query("SELECT 'foo', generate_series(1, 10)")
    ]
    assert result == [('foo', i) for i in range(1, 11)]
