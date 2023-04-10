from numbers import Number

from charging_stations_pipelines.utils.logging_utils import log


def check_coordinates(coords: float) -> float:
    if isinstance(coords, str):
        log.warn(f"Coords are string: {coords} will be transformed!")
        coords = float(
            "".join([s for s in coords.replace(",", ".") if (s.isdigit()) | (s == ".") | (s == "-")])
        )
    if not isinstance(coords, Number):
        raise ValueError("Coordinatess could not be read propery!")
    return coords