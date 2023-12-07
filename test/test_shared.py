"""Unit tests for the shared pipeline functions."""
from datetime import datetime

import pandas as pd
import pytest

from charging_stations_pipelines.shared import (
    check_coordinates,
    float_cmp_eq,
    lst_expand,
    lst_filter_none,
    lst_flatten,
    parse_date,
    str_clean_pattern,
    str_split_pattern,
    str_strip_whitespace,
    str_to_float,
    try_remove_dupes,
)


def test_check_coordinates():
    assert float_cmp_eq(check_coordinates("52.52"), 52.52)
    assert float_cmp_eq(check_coordinates("-52.52"), -52.52)
    assert float_cmp_eq(check_coordinates(3.14), 3.14)
    assert float_cmp_eq(check_coordinates(3), 3.0)
    assert float_cmp_eq(check_coordinates(48.0449426), 48.0449426)
    assert float_cmp_eq(check_coordinates(-1.602638), -1.602638)

    assert check_coordinates(None) is None

    with pytest.raises(ValueError):
        check_coordinates("N/A")

    with pytest.raises(ValueError):
        check_coordinates("")

    with pytest.raises(ValueError):
        check_coordinates("   ")


def test_str_parse_date():
    assert parse_date("2022-01-01") == datetime(2022, 1, 1)
    assert parse_date("2023-03-29T17:45:00Z").isoformat() == "2023-03-29T17:45:00+00:00"
    assert parse_date(None) is None
    assert parse_date("abc") is None


def test_str_strip_whitespace():
    assert str_strip_whitespace("    test    ") == "test"
    assert str_strip_whitespace(pd.Series(["    test1    ", "   test2   "])) == [
        "test1",
        "test2",
    ]
    assert str_strip_whitespace("   ") == ""
    assert str_strip_whitespace(None) == ""
    assert str_strip_whitespace(None, default=None) is None
    assert str_strip_whitespace(None, default="abc") == "abc"


def test_str_parse_float():
    assert float_cmp_eq(str_to_float("5.5"), 5.5)
    assert str_to_float(None) is None
    assert str_to_float("") is None
    assert str_to_float("   ") is None
    assert str_to_float(" abc  ") is None


def test_str_clean_pattern():
    # remove patterns from string
    assert str_clean_pattern("Hello world", "world") == "Hello"
    assert str_clean_pattern("Hello World", "world") == "Hello"
    # string doesn't contain the pattern
    assert str_clean_pattern("Lazy fox", "dog") == "Lazy fox"
    # remove leading and trailing whitespace
    assert str_clean_pattern(" Hello world ", "world") == "Hello"
    # raw string is None
    assert str_clean_pattern(None, "world") is None
    assert str_clean_pattern(None, None) is None


@pytest.mark.parametrize(
    ids=[
        "pattern_split",
        "no_matches",
        "empty_string",
        "none_input_1",
        "none_input_2",
        "strip_spaces",
    ],
    argnames="input_str,split_pattern,expected",
    argvalues=[
        ("hello, world, python, testing", ",", ["hello", "world", "python", "testing"]),
        ("hello world python testing", ",", ["hello world python testing"]),
        ("", ",", [""]),
        (None, ",", []),
        (None, None, []),
        (
            "    hello   , world , python , testing    ",
            ",",
            ["hello", "world", "python", "testing"],
        ),
    ],
)
def test_str_split_pattern(input_str, split_pattern, expected):
    assert str_split_pattern(input_str, split_pattern) == expected


def test_lst_flatten():
    assert lst_flatten([[1, 2, 3], [4, 5, 6]]) == [1, 2, 3, 4, 5, 6]
    assert lst_flatten([[1, 2, 3], None]) == [1, 2, 3, None]
    assert lst_flatten([[1, 2, 3], []]) == [1, 2, 3]
    assert lst_flatten([[], [1, 2, 3]]) == [1, 2, 3]
    assert lst_flatten([[1, 2, 3], [1, 2, 3]]) == [1, 2, 3, 1, 2, 3]
    assert lst_flatten([[5, 4, 3], [2, 1, 0]]) == [5, 4, 3, 2, 1, 0]
    assert lst_flatten(None) == []
    assert lst_flatten([]) == []
    assert lst_flatten([[], [], []]) == []
    assert lst_flatten([None, None]) == [None, None]
    assert lst_flatten([[None, None, None], [None, None]]) == [
        None,
        None,
        None,
        None,
        None,
    ]
    assert lst_flatten([[1, 2, [3, 4]], [5, [[6]]]]) == [1, 2, 3, 4, 5, 6]


def test_lst_remove_dupes():
    assert try_remove_dupes([1, 1, 2, 3]) == [1, 2, 3]
    assert try_remove_dupes(None) == []


def test_lst_expand():
    assert lst_expand([]) == []
    assert lst_expand([(2.5, 1)]) == [2.5]
    assert lst_expand([(1.0, 3), (2.5, 2)]) == [1.0, 1.0, 1.0, 2.5, 2.5]
    assert (
        lst_expand([(1.0, 1500), (3.9, 20), (2.0, 5)])
        == [1.0] * 1500 + [3.9] * 20 + [2.0] * 5
    )

    assert (
        lst_expand([(6.75, 2), (4.0, 100), (1.5, 35)])
        == [6.75] * 2 + [4.0] * 100 + [1.5] * 35
    )
    assert lst_expand([(1.0, 3), (2.5, 2)]) == [1.0, 1.0, 1.0, 2.5, 2.5]


def test_lst_filter_none():
    assert lst_filter_none([]) == []
    assert lst_filter_none([None, None, None]) == []
    assert lst_filter_none([1, 2, 3]) == [1, 2, 3]
    assert lst_filter_none([1, None, 2, None, 3]) == [1, 2, 3]
    assert lst_filter_none(["a", None, "b", None, "c"]) == ["a", "b", "c"]
    assert lst_filter_none(None) == []
