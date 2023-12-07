"""Unit tests for the shared pipeline functions."""

from datetime import datetime

import pandas as pd
import pytest

from charging_stations_pipelines.shared import check_coordinates, lst_expand, lst_flatten, parse_date, \
    str_strip_whitespace, str_to_float, try_remove_dupes
from test.shared import is_float_eq


def test_check_coordinates():
    assert is_float_eq(check_coordinates("52.52"), 52.52)
    assert is_float_eq(check_coordinates("-52.52"), -52.52)
    assert is_float_eq(check_coordinates(3.14), 3.14)
    assert is_float_eq(check_coordinates(3), 3.0)
    with pytest.raises(ValueError):
        check_coordinates("N/A")


def test_parse_date():
    assert parse_date('2022-01-01') == datetime(2022, 1, 1)
    assert parse_date("2023-03-29T17:45:00Z").isoformat() == "2023-03-29T17:45:00+00:00"
    assert parse_date(None) is None
    assert parse_date('abc') is None


def test_str_strip_whitespace():
    assert str_strip_whitespace("    test    ") == "test"
    assert str_strip_whitespace(pd.Series(["    test1    ", "   test2   "])) == ["test1", "test2"]
    assert str_strip_whitespace(["    test1    ", "   test2   "]) == ["test1", "test2"]


def test_str_to_float():
    assert is_float_eq(str_to_float("5.5"), 5.5)
    assert str_to_float(None) is None


def test_lst_flatten():
    assert lst_flatten([[1, 2, 3], [4, 5, 6]]) == [1, 2, 3, 4, 5, 6]
    assert lst_flatten([[1, 2, 3], None]) == [1, 2, 3]


def test_try_remove_dupes():
    assert try_remove_dupes([1, 1, 2, 3]) == [1, 2, 3]
    assert try_remove_dupes(None) == []


def test_lst_expand():
    assert lst_expand([]) == []
    assert lst_expand([(2.5, 1)]) == [2.5]
    assert lst_expand([(1.0, 3), (2.5, 2)]) == [1.0, 1.0, 1.0, 2.5, 2.5]
    assert lst_expand([(1.0, 1500), (3.9, 20), (2.0, 5)]) == [1.0] * 1500 + [3.9] * 20 + [2.0] * 5
    assert lst_expand([(6.75, 2), (4.0, 100), (1.5, 35)]) == [6.75] * 2 + [4.0] * 100 + [1.5] * 35
    assert lst_expand([(1.0, 3), (2.5, 2)]) == [1.0, 1.0, 1.0, 2.5, 2.5]
