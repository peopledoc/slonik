import math
import uuid

import pytest


def test_int(conn):
    assert conn.get_one('SELECT 42') == 42
    assert conn.get_one('SELECT -2147483648') == -2147483648
    assert conn.get_one("SELECT '21'::int") == 21
    assert conn.get_one("SELECT 0") == 0


def test_bigint(conn):
    assert conn.get_one('SELECT 2147483648') == 2147483648
    assert conn.get_one('SELECT 9223372036854775807') == 9223372036854775807
    assert conn.get_one('SELECT -9223372036854775808') == -9223372036854775808


def test_text(conn):
    assert conn.get_one("SELECT 'foo'") == 'foo'
    assert conn.get_one("SELECT ''") == ''
    assert conn.get_one("SELECT '123'") == '123'
    assert conn.get_one("SELECT 42::text") == '42'
    assert conn.get_one("SELECT 'quote o''clock'") == "quote o'clock"
    assert conn.get_one("SELECT 'ùñî©òðề'") == 'ùñî©òðề'

    assert conn.get_one("SELECT 'foo'::char(5)") == 'foo  '

    assert conn.get_one("SELECT 'foo'::varchar") == 'foo'
    assert conn.get_one("SELECT 'ùñî©òðề'::varchar") == 'ùñî©òðề'


def test_float(conn):
    assert conn.get_one('SELECT 42.1::float') == 42.1
    assert conn.get_one('SELECT 1 / 3::float') == 1 / 3
    assert conn.get_one('SELECT -1 / 2::float') == -0.5
    assert conn.get_one("SELECT '-Infinity'::float") == float('-Infinity')
    assert math.isnan(conn.get_one("SELECT 'inf'::float - 'inf'::float"))


@pytest.mark.xfail(reason='Not implemented yet')
def test_numeric(conn):
    assert conn.get_one('SELECT 42.1') == 42.1
    assert conn.get_one('SELECT 1. / 3') == 1. / 3
    assert conn.get_one('SELECT -1. / 2') == -0.5
    assert conn.get_one("SELECT '-Infinity'::") == float('-Infinity')
    assert math.isnan(conn.get_one("SELECT 'inf'::float - 'inf'::float"))


def test_json(conn):
    result = conn.get_one('''SELECT '{"a": [1, 2, 3]}'::json''')
    assert result == {'a': [1, 2, 3]}

    result = conn.get_one('''SELECT '[{}, 42, [null]]'::json''')
    assert result == [{}, 42, [None]]


def test_jsonb(conn):
    result = conn.get_one('''SELECT '{"a": [1, 2, 3]}'::jsonb''')
    assert result == {'a': [1, 2, 3]}

    result = conn.get_one('''SELECT '[{}, 42, [null]]'::jsonb''')
    assert result == [{}, 42, [None]]


def test_uuid(conn):
    res = conn.get_one("SELECT '3d9d291d-8668-480f-98bf-46ee10d07a5d'::uuid")
    assert res == uuid.UUID('3d9d291d-8668-480f-98bf-46ee10d07a5d')
