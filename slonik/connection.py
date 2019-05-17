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


def get_deserializer(fmt):
    fmt = '>' + fmt

    def unpack(value):
        ret, = struct.unpack(fmt, value)
        return ret

    return unpack


def get_serializer(typename, fmt):
    fmt = '>' + fmt

    def pack(value):
        return typename, struct.pack(fmt, value)

    return pack


class _Result(rust.RustObject):
    def next_row(self):
        row = self._methodcall(lib.next_row)
        if row == ffi.NULL:
            return
        return _Row._from_objptr(row)

    def close(self):
        self._methodcall(lib.result_close)


class Result:
    def __init__(self, _result):
        self._result = _result

    def __iter__(self):
        return self

    def __next__(self):
        _row = self._result.next_row()
        if _row is None:
            raise StopIteration

        with Row(_row) as row:
            return tuple(row)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._result.close()


class _Row(rust.RustObject):
    def len(self):
        return self._methodcall(lib.row_len)

    def item(self, i):
        item = self._methodcall(lib.row_item, i)
        typename = rust.buff_to_bytes(item.typename)
        value = rust.buff_to_bytes(item.value)
        return typename, value

    def close(self):
        self._methodcall(lib.row_close)


class Row:
    deserializers = {
        b'int2': get_deserializer('h'),
        b'int4': get_deserializer('i'),
        b'int8': get_deserializer('q'),
        b'float8': get_deserializer('d'),
        b'text': lambda value: value.decode(),
        b'unknown': lambda value: value.decode(),
        b'bpchar': lambda value: value.decode(),
        b'varchar': lambda value: value.decode(),
        b'json': json.loads,
        b'jsonb': lambda value: json.loads(value[1:]),  # always start with '1'
        b'uuid': lambda value: uuid.UUID(bytes=value),
    }

    def __init__(self, _row):
        self._row = _row
        self._len = None

    def __len__(self):
        if self._len is None:
            self._len = self._row.len()
        return self._len

    def __getitem__(self, i):
        if not 0 <= i < len(self):
            raise IndexError(f'Index {i!r} out fo range')

        typename, value = self._row.item(i)
        if typename is None:
            return

        deserializer = self.deserializers.get(typename)
        if deserializer is not None:
            value = deserializer(value)

        return value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._row.close()


class _Query(rust.RustObject):
    def add_param(self, typename: bytes, value: bytes):
        typename = ffi.from_buffer(typename)
        value = ffi.from_buffer(value)
        self._methodcall(lib.query_param, ((len(typename), typename), (len(value), value)))

    def execute(self):
        self._methodcall(lib.query_exec)

    def execute_result(self):
        result = self._methodcall(lib.query_exec_result)
        return _Result._from_objptr(result)


class Query:
    serializers = {
        int: get_serializer(b'int4', 'i'),
        str: lambda s: (b'str', s.encode()),
    }

    def __init__(self, _query):
        self._query = _query
        self.params = []

    def add_param(self, param):
        serializer = self.serializers[type(param)]
        typename, value = serializer(param)

        self.params.append((typename, value))
        self._query.add_param(typename, value)

    def add_params(self, params):
        for param in params:
            self.add_param(param)

    def execute(self):
        self._query.execute()

    def execute_result(self):
        with Result(self._query.execute_result()) as result:
            yield from result


class _Conn(rust.RustObject):

    @classmethod
    def from_dsn(cls, dsn: bytes):
        result = rust.call(lib.connect, dsn, len(dsn))
        rv = cls._from_objptr(result)
        return rv

    def close(self):
        self._methodcall(lib.close)

    def new_query(self, sql: bytes):
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
