import logging
from dateutil import parser
from numbers import Number

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
