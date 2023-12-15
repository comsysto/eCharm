"""Module containing shared utility functions for the charging stations pipelines."""

import logging
import re
from collections.abc import Iterable
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence, TypeVar, Union

import pandas as pd
from dateutil import parser

logger = logging.getLogger(__name__)


JSON = Union[Sequence['JSON'], Mapping[str, 'JSON'], bool, int, float, str, None]
"""A type alias for a JSON object."""

T = TypeVar("T")


def check_coordinates(coords: Optional[Union[float, int, str]]) -> Optional[float]:
    """Helper function to convert string coordinates to float.
    It handles coordinates given as strings, and replaces commas with dots, and keeps only digits, dots and minus sign.

    :param coords: float, int ot string that represents a coordinate
    :return: coordinate as float
    """
    if coords is None:
        return None

    if isinstance(coords, str):
        try:
            processed_coords = "".join(
                c for c in coords.replace(",", ".") if c.isdigit() or c in ".-"
            )
            logger.debug(f"Coords are string: {coords} will be transformed!")
            return float(processed_coords)
        except (ValueError, TypeError):
            pass  # will raise ValueError later

    try:
        return float(coords)
    except (ValueError, TypeError):
        pass  # will raise ValueError later

    raise ValueError("Coordinates could not be read properly!")


def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parses a string representation of a date into a datetime object.

    :param date_str: The string representation of a date.
    :return: A datetime object representing the parsed date, or None if the date could not be parsed.
    """
    if not date_str:
        return None

    try:
        return parser.parse(date_str)
    except (parser.ParserError, TypeError) as _:
        return None


def str_to_float(s: Optional[Union[str, float, int]]) -> Optional[float]:
    """Converts input string to a float number.

    :param s: The input value that will be attempted to convert into a float.
    :return: The float representation of the input value, or None if the conversion fails.
    """
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def str_to_bool(bool_string: str) -> bool:
    """Converts a string to a boolean value."""
    return bool_string.lower() in ["True", "true", "1", "t"]


def str_strip_whitespace(
    s: Union[Optional[str], list[Optional[str]], pd.Series], default=""
) -> Union[Optional[str], list[Optional[str]]]:
    """Strips whitespace from strings in a given input.

    :param s: A string, list of strings or pandas Series containing strings.
    :param default: Default value to return if input is not a string, list or Series. Default is an empty string.
    :return: The input string with whitespace stripped.
    """
    if isinstance(s, str):
        return s.strip()
    elif isinstance(s, pd.Series):
        return str_strip_whitespace(s.to_list(), default)
    elif isinstance(s, list):
        return [str_strip_whitespace(item, default) for item in s]
    else:
        return default


def str_clean_pattern(
    raw_str: Optional[str], remove_pattern: Optional[str]
) -> Optional[str]:
    """Removes a given pattern from a string."""
    return (
        re.sub(remove_pattern, "", raw_str, flags=re.IGNORECASE).strip()
        if raw_str and remove_pattern
        else None
    )


def str_split_pattern(raw_str: Optional[str], split_pattern: str) -> list[str]:
    """Splits a string into a list of strings using a given pattern."""
    if raw_str is None:
        return []
    split_list = re.split(split_pattern, raw_str)
    return list(map(str.strip, split_list))


def lst_flatten(nested_list: Optional[list[Optional[list[T]]]]) -> list[T]:
    """Flattens a nested list into a single level list.

    :param nested_list: A nested list of any type.
    :return: A flattened list.
    """

    def _flatten_rec(lst):
        """Helper function to flatten a nested list recursively."""
        if not lst:
            return []

        result = []
        for elem in lst:
            if isinstance(elem, list):
                result.extend(_flatten_rec(elem))
            else:
                result.append(elem)
        return result

    return _flatten_rec(nested_list)


def try_remove_dupes(lst: Optional[list[T]], default=None) -> Optional[list[T]]:
    """Removes duplicate values from the list.

    :param lst: The list from which duplicates need to be removed.
    :param default: The default value to be returned if lst is None.
    :return: A new list with duplicates removed. If lst is None,
        returns the default value (or an empty list if not provided).
    """
    return list(dict.fromkeys(lst)) if lst else (default or [])


def float_cmp_eq(a, b, eps=1e-10):
    """Check if two floating-point numbers are close to each other (i.e. "equal")."""
    if a is None or b is None:
        return False
    return abs(a - b) <= eps


def lst_filter_none(lst: Optional[Iterable[Any]]) -> list[Any]:
    """Filter out None elements from a given iterable list."""
    return [e for e in lst if e is not None] if lst else []


def lst_expand(aggregated_list: list[tuple[float, int]]) -> list[float]:
    """Repeats float value according to its count in the input list.

    :param aggregated_list: A list of tuples where each tuple contains a float value and the count of how often
        the value occurs.
    :return: A new list where each float value is repeated according to its count in the input list.

    Example:
    >>> lst_expand([(1.0, 3), (2.5, 2)])
    [1.0, 1.0, 1.0, 2.5, 2.5]
    """
    # [0] - float value, [1] - count, how often this value occurs
    return [e[0] for e in aggregated_list for _ in range(e[1])] if aggregated_list else []


def coalesce(*args) -> Any:
    """Returns the first non-empty argument."""
    for arg in args:
        if arg is not None and arg != '':
            return arg
    return None


def reject_if(condition: bool, error_message="") -> None:
    """Raises a RuntimeError if the given condition is True."""
    if condition:
        raise RuntimeError(error_message)
