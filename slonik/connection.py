import json
import os
import struct
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


class Connection:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.__conn = None

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
