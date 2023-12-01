"""Module containing shared utility functions for the charging stations pipelines."""

import logging
from datetime import datetime
from numbers import Number
from typing import Optional, TypeVar, Union

import pandas as pd
from dateutil import parser

logger = logging.getLogger(__name__)

T = TypeVar("T")


def check_coordinates(coords: Union[float, int, str]) -> Optional[float]:
    """Helper function to convert string coordinates to float.
    It handles coordinates given as strings, and replaces commas with dots, and keeps only digits, dots and minus sign.

    :param coords: float, int ot string that represents a coordinate
    :return: coordinate as float
    """
    if coords is None:
        return None

    if isinstance(coords, str):
        try:
            processed_coords = "".join(c for c in coords.replace(",", ".") if c.isdigit() or c in ".-")
            logger.debug(f"Coords are string: {coords} will be transformed!")
            return float(processed_coords)
        except ValueError:
            pass  # will raise ValueError later

    if isinstance(coords, Number):
        try:
            return float(coords)
        except ValueError:
            pass  # will raise ValueError later

    raise ValueError("Coordinates could not be read properly!")


def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parses a string representation of a date into a datetime object.

    :param date_str: The string representation of a date.
    :return: A datetime object representing the parsed date, or None if the date could not be parsed.
    """
    try:
        return parser.parse(date_str)
    except TypeError as e:
        logger.debug(f"Could not parse date string: '{date_str}'! {e}")
        return None


def str_strip_whitespace(s: Union[Optional[str], list[Optional[str]], pd.Series], default='') \
        -> Union[Optional[str], list[Optional[str]]]:
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


def str_to_float(s: Optional[str]) -> Optional[float]:
    """Converts input string to a float number.

    :param s: The input value that will be attempted to convert into a float.
    :return: The float representation of the input value, or None if the conversion fails.
    """
    try:
        return float(s)
    except TypeError:
        return None


def lst_flatten(nested_list: list[list[T]]) -> list[T]:
    """Flattens a nested list into a single level list.

    :param nested_list: A nested list of any type.
    :return: A flattened list.
    """
    return [item for sublist in nested_list for item in (sublist or [])] if nested_list else []


def try_remove_dupes(lst: Optional[list[T]], default=None) -> Optional[list[T]]:
    """Removes duplicate values from the list.

    :param lst: The list from which duplicates need to be removed.
    :param default: The default value to be returned if lst is None.
    :return: A new list with duplicates removed. If lst is None,
        returns the default value (or an empty list if not provided).
    """
    return list(dict.fromkeys(lst)) if lst else (default or [])
