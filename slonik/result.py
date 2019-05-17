from slonik import rust
from slonik._native import ffi
from slonik._native import lib

from .row import _Row
from .row import Row


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
