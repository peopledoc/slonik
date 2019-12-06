import asyncio
import concurrent
import functools
import json
import os
import struct
import threading
import uuid
from typing import Any
from typing import Iterable
from typing import Tuple

from slonik import rust
from slonik._native import ffi
from slonik._native import lib

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
        self._pool = set()
        self._pool_lock = threading.Lock()
        self._local = threading.local()

    @property
    def _conn(self):
        conn = getattr(self._local, 'conn', None)
        if conn is None:
            conn = self._local.conn = Connection(self.dsn)
            with self._pool_lock:
                self._pool.add(conn)
        return conn

    def close(self):
        self._executor.shutdown()
        with self._pool_lock:
            pool, self._pool = self._pool, None
        for conn in pool:
            conn.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self):
        self.close()

    def _run(self, method, args, kwargs):
        return method(self._conn, *args, **kwargs)

    async def _async_run(self, method, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._run,
            method,
            args,
            kwargs,
        )

    async def execute(self, sql: str, *args):
        await self._async_run(Connection.execute, sql, *args)

    async def query(self, sql: str, *args) -> Iterable[Tuple[Any]]:
        rows = await self._async_run(Connection.query, sql, *args)
        for row in rows:
            yield row

    async def get_one(self, sql: str, *args) -> Tuple[Any]:
        return await self._async_run(Connection.get_one, sql, *args)

    async def get_value(self, sql: str, *args) -> Any:
        return await self._async_run(Connection.get_value, sql, *args)
