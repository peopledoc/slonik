import json
import os
import struct
import uuid
from typing import Iterable

from slonik import rust
from slonik._native import lib


class Row():

    def __init__(self, row):
        self._row = row


def buff_to_bytes(buff):
    if not buff.bytes:
        return None
    return bytes(buff.bytes[0:buff.size])


def converter(fmt):
    fmt = '>' + fmt

    def unpack(value):
        ret, = struct.unpack(fmt, value)
        return ret

    return unpack


class _Conn(rust.RustObject):

    @classmethod
    def from_dsn(cls, dsn):
        dsn = dsn.encode('utf-8')
        rv = cls._from_objptr(rust.call(lib.connect, dsn, len(dsn)))

        return rv

    types = {
        b'int2': converter('h'),
        b'int4': converter('i'),
        b'int8': converter('q'),
        b'float8': converter('d'),
        b'text': lambda value: value.decode(),
        b'unknown': lambda value: value.decode(),
        b'bpchar': lambda value: value.decode(),
        b'varchar': lambda value: value.decode(),
        b'json': json.loads,
        b'jsonb': lambda value: json.loads(value[1:]),  # always start with '1'
        b'uuid': lambda value: uuid.UUID(bytes=value),
    }

    def query(self, sql: str):
        sql = sql.encode('utf-8')
        rows = self._methodcall(lib.query, sql, len(sql))
        rows_iter = rust.call(lib.rows_iterator, rows)

        while True:
            row = rust.call(lib.next_row, rows_iter)
            typename = buff_to_bytes(row.typename)
            if typename is None:
                break

            value = buff_to_bytes(row.value)
            type_ = self.types.get(typename)
            if type_ is not None:
                value = type_(value)

            yield value


class Connection():

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
            self.__conn = _Conn.from_dsn(self.dsn)

        return self.__conn

    def query(self, sql: str) -> Iterable[Row]:
        _rows = self._conn.query(sql)

        for row in _rows:
            yield row

    def get_one(self, sql: str) -> Row:
        return next(self.query(sql))
