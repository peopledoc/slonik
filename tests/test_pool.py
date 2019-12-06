import asyncio
import uuid

import pytest

from slonik.pool import AsyncPool


pytestmark = pytest.mark.asyncio


async def test_pool():
    created_objects = set()
    destructed_objects = set()

    async def ctor():
        obj = uuid.uuid4()
        created_objects.add(obj)
        return obj

    async def dtor(obj):
        assert obj in created_objects
        assert obj not in destructed_objects
        destructed_objects.add(obj)

    pool = AsyncPool(ctor, dtor)

    async with pool.get() as obj1:
        assert created_objects == {obj1}

    async with pool.get() as obj2:
        assert obj1 is obj2
        assert created_objects == {obj1}

    async with pool.get() as obj1:
        async with pool.get() as obj2:
            assert obj1 is not obj2
            assert created_objects == {obj1, obj2}

    await pool.close()
    assert destructed_objects == created_objects


async def test_pool_max_instances():
    created_objects = []
    enter_event = asyncio.Event()
    exit_event = asyncio.Event()

    async def ctor():
        obj = uuid.uuid4()
        created_objects.append(obj)
        return obj

    async def runner():
        async with pool.get():
            enter_event.set()
            await exit_event.wait()

    async def checker():
        await enter_event.wait()
        exit_event.set()

    pool = AsyncPool(ctor, max_instances=2)
    await asyncio.gather(
        runner(), runner(), runner(), runner(),
        checker()
    )
    assert len(created_objects) == 2

    created_objects.clear()
    enter_event.clear()
    exit_event.clear()
    pool = AsyncPool(ctor, max_instances=3)
    await asyncio.gather(
        runner(), runner(), runner(), runner(),
        checker(),
    )
    assert len(created_objects) == 3
