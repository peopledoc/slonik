import struct

from slonik import rust
from slonik._native import ffi
from slonik._native import lib

from .result import _Result
from .result import Result


def get_serializer(typename, fmt):
    fmt = '>' + fmt

    def pack(value):
        return typename, struct.pack(fmt, value)

    return pack


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

    # TODO: Add a close method called if Query is cancelled (or called everytime?)


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
