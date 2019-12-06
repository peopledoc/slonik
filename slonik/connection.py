import asyncio
import concurrent
import os
from contextlib import asynccontextmanager
from typing import Any
from typing import Iterable
from typing import Tuple

from slonik import rust
from slonik._native import lib

from .pool import AsyncPool
from .query import _Query
from .query import Query


class _Conn(rust.RustObject):

    @classmethod
    def connect(cls, dsn: bytes):
        conn = rust.call(lib.connect, dsn, len(dsn))
        return cls._from_objptr(conn)

    def close(self):
        self._methodcall(lib.close)

    def new_query(self, sql: bytes):
        query = self._methodcall(lib.new_query, sql, len(sql))
        return _Query._from_objptr(query)


class BaseConnection:
    def __init__(self, dsn: str):
        self.dsn = dsn

    @classmethod
    def from_env(cls, pghost: str = '', pgport: str = '', pguser: str = '',
                 pgpassword: str = '', pgdatabase: str = '',
                 pgoptions: str = ''):
        pghost = pghost or os.environ.get('PGHOST', 'localhost')
        pgport = pgport or os.environ.get('PGPORT', '5432')
        pguser = pguser or os.environ.get('PGUSER', 'postgres')
        pgpassword = pgpassword or os.environ.get('PGPASSWORD', 'postgres')
        pgdatabase = pgdatabase or os.environ.get('PGDATABASE', 'postgres')
        pgoptions = pgoptions or os.environ.get('PGOPTIONS', '')

        dsn = (
            f'postgresql://{pguser}:{pgpassword}@{pghost}:{pgport}'
            f'/{pgdatabase}?{pgoptions}'
        )
        return cls(dsn)

    def close(self):
        raise NotImplementedError


class Connection(BaseConnection):
    def __init__(self, dsn: str):
        super().__init__(dsn)
        self.__conn = None

    @property
    def _conn(self):
        if not self.__conn:
            self.__conn = _Conn.connect(self.dsn.encode('utf-8'))

        return self.__conn

    def close(self):
        if self.__conn is not None:
            self.__conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _get_query(self, sql: str, params):
        sql = sql.encode('utf-8')
        query = Query(self._conn.new_query(sql))
        query.add_params(params)
        return query

    def execute(self, sql: str, *args):
        query = self._get_query(sql, args)
        query.execute()

    def query(self, sql: str, *args) -> Iterable[Tuple[Any]]:
        query = self._get_query(sql, args)
        yield from query.execute_result()

    def get_one(self, sql: str, *args) -> Tuple[Any]:
        return next(self.query(sql, *args))

    def get_value(self, sql: str, *args) -> Any:
        value, = self.get_one(sql, *args)
        return value


class AsyncConnection(BaseConnection):
    def __init__(self, dsn):
        super().__init__(dsn)
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self._pool = AsyncPool(self._get_conn, self._close_conn, max_instances=4)

    async def close(self):
        self._executor.shutdown()
        await self._pool.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self):
        await self.close()

    async def _get_conn(self):
        # Create a new connection for the pool
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            _Conn.connect,
            self.dsn.encode('utf-8'),
        )

    async def _close_conn(self, conn):
        conn.close()

    @asynccontextmanager
    async def _get_query(self, loop, sql: str, params):
        sql = sql.encode('utf-8')
        async with self._pool.get() as conn:
            query = Query(await loop.run_in_executor(
                self._executor,
                conn.new_query,
                sql,
            ))
            await loop.run_in_executor(
                self._executor,
                query.add_params,
                params,
            )
            yield query

    async def execute(self, sql: str, *args):
        loop = asyncio.get_running_loop()
        async with self._get_query(loop, sql, args) as query:
            await loop.run_in_executor(self._executor, query.execute)

    async def query(self, sql: str, *args) -> Iterable[Tuple[Any]]:
        loop = asyncio.get_running_loop()
        async with self._get_query(loop, sql, args) as query:
            rows = await loop.run_in_executor(
                self._executor,
                query.execute_result,
            )
            try:
                while True:
                    row = await loop.run_in_executor(
                        self._executor,
                        next,
                        rows,
                        None,
                    )
                    if row is None:
                        break
                    yield row
            except GeneratorExit:
                pass

    async def get_one(self, sql: str, *args) -> Tuple[Any]:
        it = self.query(sql, *args)
        return await it.__anext__()

    async def get_value(self, sql: str, *args) -> Any:
        value, = await self.get_one(sql, *args)
        return value
