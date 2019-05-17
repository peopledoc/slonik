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


def converter(fmt):
    fmt = '>' + fmt

    def unpack(value):
        ret, = struct.unpack(fmt, value)
        return ret

    return unpack


class _Query(rust.RustObject):
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
    def add_param(self, type_: bytes, value: bytes):
        self._methodcall(lib.query_param, ((len(type_), type_), (len(value), value)))

    def execute(self):
        self._methodcall(lib.query_exec)

    def execute_result(self):
        result = self._methodcall(lib.query_exec_result)

        def _get_row_item(row, i):
            item = row._methodcall(lib.row_item, i)
            typename = rust.buff_to_bytes(item.typename)
            if typename is None:
                return

            value = rust.buff_to_bytes(item.value)
            type_ = self.types.get(typename)
            if type_ is not None:
                value = type_(value)

            return value

        try:
            while True:
                # Use RustObject for row
                row = rust.call(lib.next_row, result)
                if row == ffi.NULL:
                    break

                row = _Row._from_objptr(row)
                items = tuple(
                    _get_row_item(row, i)
                    for i in range(row._methodcall(lib.row_len))
                )
                row._methodcall(lib.row_close)
                yield items

        finally:
            rust.call(lib.result_close, result)



class Query:
    def __init__(self, _query):
        self._query = _query
        self.params = []

    def add_param(self, param):
        import struct
        if isinstance(param, int):
            t = ffi.from_buffer(b'int4')
            p = ffi.from_buffer(struct.pack('>i', param))
        elif isinstance(param, str):
            t = ffi.from_buffer(b'text')
            p = ffi.from_buffer(param.encode())

        self.params.append((t, p))
        self._query.add_param(t, p)

    def add_params(self, params):
        for param in params:
            self.add_param(param)

    def execute(self):
        self._query.execute()

    def execute_result(self):
        for row in self._query.execute_result():
            yield row


class _Row(rust.RustObject):
    pass


class _Conn(rust.RustObject):

    @classmethod
    def from_dsn(cls, dsn: bytes):
        result = rust.call(lib.connect, dsn, len(dsn))
        rv = cls._from_objptr(result)
        return rv

    def close(self):
        self._methodcall(lib.close)

    def get_query(self, sql: bytes):
        query = self._methodcall(lib.new_query, sql, len(sql))
        return _Query._from_objptr(query)


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
            self.__conn = _Conn.from_dsn(self.dsn.encode('utf-8'))

        return self.__conn

    def close(self):
        if self.__conn is not None:
            self.__conn.close()

    def _get_query(self, sql: str, params):
        sql = sql.encode('utf-8')
        query = Query(self._conn.get_query(sql))
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
