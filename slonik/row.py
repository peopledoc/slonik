import json
import struct
import uuid

from slonik import rust
from slonik._native import ffi
from slonik._native import lib


def get_deserializer(fmt):
    fmt = '>' + fmt

    def unpack(value):
        ret, = struct.unpack(fmt, value)
        return ret

    return unpack


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
            if self._row is None:
                self._len = 0
            else:
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

    def close(self):
        self._row.close()
        self._row = None
        self._len = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
