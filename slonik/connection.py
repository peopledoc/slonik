from typing import Iterable

from slonik import rust
from slonik._native import lib


class Row():

    def __init__(self, row):
        self._row = row


class _Conn(rust.RustObject):

    @classmethod
    def from_dsn(cls, dsn):
        dsn = dsn.encode('utf-8')
        rv = cls._from_objptr(rust.call(lib.connect, dsn, len(dsn)))

        return rv

    def query(self, sql: str):
        sql = sql.encode('utf-8')
        rows = self._methodcall(lib.query, sql, len(sql))
        rows_iter = rust.call(lib.rows_iterator, rows)

        while True:
            row = rust.call(lib.next_row, rows_iter)
            if not row.valid:
                break
            yield row.value


class Connection():

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.__conn = None

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
