import sys
import pathlib

import pytest

# Ensure project root is on sys.path so `Utils` can be imported during tests
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from Utils.Resolve import Metadata


def test_int_stays_int_from_numeric_string():
    meta = Metadata(type_="int")
    value = meta.resolveValue("5")
    assert value == 5
    assert meta.type == "int"


def test_int_to_float_when_int_parse_fails():
    meta = Metadata(type_="int")
    value = meta.resolveValue("5.5")
    assert value == 5.5
    assert meta.type == "float"


def test_int_to_list_int():
    meta = Metadata(type_="int")
    value = meta.resolveValue("[1, 2, 3]")
    assert value == [1, 2, 3]
    assert meta.type == "list"
    assert meta.subtype is not None
    assert meta.subtype.type == "int"


def test_int_to_list_float():
    meta = Metadata(type_="int")
    value = meta.resolveValue("[1.2, 2.3]")
    assert value == [1.2, 2.3]
    assert meta.type == "list"
    assert meta.subtype is not None
    assert meta.subtype.type == "float"


def test_float_to_list_float():
    meta = Metadata(type_="float")
    value = meta.resolveValue("[1.2, 2.3]")
    assert value == [1.2, 2.3]
    assert meta.type == "list"
    assert meta.subtype is not None
    assert meta.subtype.type == "float"


def test_bool_to_int_when_not_bool():
    meta = Metadata(type_="bool")
    value = meta.resolveValue("2")
    assert value == 2
    assert meta.type == "int"


def test_list_int_to_list_float():
    meta = Metadata(type_="list")
    meta.subtype = Metadata(type_="int")
    value = meta.resolveValue("[1.5, 2.5]")
    assert value == [1.5, 2.5]
    assert meta.type == "list"
    assert meta.subtype is not None
    assert meta.subtype.type == "float"


def test_list_float_to_list_str():
    meta = Metadata(type_="list")
    meta.subtype = Metadata(type_="float")
    value = meta.resolveValue("[\"a\", \"b\"]")
    assert value == ["a", "b"]
    assert meta.type == "list"
    assert meta.subtype is not None
    assert meta.subtype.type == "str"
