import logging
from numbers import Number

logger = logging.getLogger(__name__)

def check_coordinates(coords: float) -> float:
    if isinstance(coords, str):
        logger.warning(f"Coords are string: {coords} will be transformed!")
        coords = float(
            "".join([s for s in coords.replace(",", ".") if (s.isdigit()) | (s == ".") | (s == "-")])
        )
    if not isinstance(coords, Number):
        raise ValueError("Coordinatess could not be read propery!")
    return coords
