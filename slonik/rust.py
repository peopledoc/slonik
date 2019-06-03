"""
Adapted from https://github.com/getsentry/semaphore/blob/886661cb2d421d3435657969c3e12e50813b8010/py/semaphore/exceptions.py
"""  # noqa

import uuid
import weakref
from slonik._native import ffi, lib
from slonik.exceptions import SlonikException


attached_refs = weakref.WeakKeyDictionary()


class _NoDict(type):

    def __new__(cls, name, bases, d):
        d.setdefault('__slots__', ())
        return type.__new__(cls, name, bases, d)


class RustObject(metaclass=_NoDict):
    __slots__ = ['_objptr', '_shared']
    __dealloc_func__ = None

    def __init__(self):
        raise TypeError('Cannot instanciate %r objects' %
                        self.__class__.__name__)

    @classmethod
    def _from_objptr(cls, ptr, shared=False):
        rv = super().__new__(cls)
        rv._objptr = ptr
        rv._shared = shared
        return rv

    def _methodcall(self, func, *args):
        return call(func, self._get_objptr(), *args)

    def _get_objptr(self):
        if not self._objptr:
            raise RuntimeError('Object is closed')
        return self._objptr

    def __del__(self):
        if self._objptr is None or self._shared:
            return
        f = self.__class__.__dealloc_func__
        if f is not None:
            call(f, self._objptr)
            self._objptr = None


def isresult(value):
    if isinstance(value, ffi.CData):
        vtype = ffi.typeof(value)
        if vtype.kind == 'struct' and vtype.cname.startswith('FFIResult_'):
            return [f for f, _ in vtype.fields] == ['status', 'data']
    return False


def call(func, *args):
    """Calls rust method and does some error handling."""
    result = func(*args)
    if isresult(result):
        if result.status:
            error = ffi.cast('_Error*', result.data)
            try:
                error_msg = buff_to_bytes(lib.error_msg(error)).decode()
                raise SlonikException(error_msg)
            finally:
                lib.error_free(error)
        result = result.data
    return result


def buff_to_bytes(buff):
    if not buff.bytes:
        return None
    return bytes(buff.bytes[0:buff.size])
