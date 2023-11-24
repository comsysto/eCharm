import logging
from numbers import Number
from typing import Optional, Any

from dateutil import parser

logger = logging.getLogger(__name__)


def check_coordinates(coords: float) -> float:
    if isinstance(coords, str):
        logger.warning(f"Coords are string: {coords} will be transformed!")
        coords = float(
            "".join([s for s in coords.replace(",", ".") if (s.isdigit()) | (s == ".") | (s == "-")])
        )
    if not isinstance(coords, Number):
        raise ValueError("Coordinatess could not be read properly!")
    return coords


def parse_date(date):
    try:
        return parser.parse(date)
    except TypeError as e:
        logger.debug(f"Could not parse DateCreated! {e}")
        return None


def try_strip_str(s: Optional[str]) -> Optional[str]:
    return str(s).strip() if s else None


def try_float(s) -> Optional[float]:
    try:
        return float(s)
    except TypeError:
        return None


def try_flatten_list(nested_list: list[list[Any]]) -> list[Any]:
    return [item for sublist in nested_list for item in sublist] if nested_list else []


def try_expand_list(aggregated_list: list[tuple[float, int]]) -> list[float]:
    # [0] - float value, [1] - count, how often this value occurs
    return [e[0] for e in aggregated_list for _ in range(e[1])] if aggregated_list else []


def try_remove_dups(lst: list[Any]) -> list[Any]:
    return list(dict.fromkeys(lst)) if lst else []
