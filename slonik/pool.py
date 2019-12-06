import asyncio
from contextlib import asynccontextmanager


class AsyncPool:
    def __init__(self, ctor, dtor=None, max_instances=4):
        assert max_instances > 0
        self.max_instances = max_instances
        self.ctor = ctor
        self.dtor = dtor
        self._pool = set()
        self._free = set()
        self._cond = asyncio.Condition()

    async def _acquire(self):
        async with self._cond:
            while True:
                if self._free:
                    # Reuse a free instance
                    return self._free.pop()
                elif len(self._pool) < self.max_instances:
                    # Create a new instance
                    obj = await self.ctor()
                    self._pool.add(obj)
                    return obj
                else:
                    # Wait for an instance to be free
                    await self._cond.wait()

    async def _release(self, obj):
        async with self._cond:
            self._free.add(obj)
            self._cond.notify()

    @asynccontextmanager
    async def get(self):
        obj = await self._acquire()
        try:
            yield obj
        finally:
            await self._release(obj)

    async def close(self):
        async with self._cond:
            pool, self._pool = self._pool, set()
        if self.dtor is not None:
            for obj in pool:
                await self.dtor(obj)
