"""This module contains the methods for mapping the OpenStreetMap (OSM) data to the database models."""
import json
import logging
from datetime import datetime
from typing import Final, Optional

from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines import osm
from charging_stations_pipelines.shared import (check_coordinates, JSON, lst_filter_none, lst_flatten,
    str_clean_pattern, str_split_pattern, str_strip_whitespace, str_to_float)

logger = logging.getLogger(__name__)

SOCKET_TYPES: Final[dict[str, str]] = {
    'socket:chademo':            'CHAdeMO',
    'socket:schuko':             'AC Schuko',
    'socket:tesla_supercharger': 'Tesla Supercharger',
    'socket:type2':              'AC Steckdose Typ 2',
    'socket:type2:combo':        'Typ 2 Combo',
    'socket:type2_combo':        'Typ 2 Combo',
    'socket:type3':              'AC Steckdose Typ 3',
    'socket:type3c':             'AC Steckdose Typ 3c',
    'socket:typee':              'AC Steckdose Typ E'}

ADDRESS_KEYS: Final[list[str]] = [
    "addr:city",
    "addr:country",
    "addr:housenumber",
    "addr:postcode",
    "addr:street",
]


def map_station_osm(entry: JSON) -> Station:
    """Maps an entry from OpenStreetMap to a Station object.

    :param entry: The entry from OpenStreetMap to be mapped.
    :return: The mapped Station object.
    """
    lat = check_coordinates(entry.get("lat"))
    lon = check_coordinates(entry.get("lon"))

    new_station = Station()
    new_station.country_code = str_strip_whitespace(entry.get("country_code")) or None
    new_station.source_id = entry.get("id") or None
    new_station.operator = (
        str_strip_whitespace(entry.get("tags", {}).get("operator")) or None
    )
    new_station.data_source = osm.DATA_SOURCE_KEY
    new_station.point = from_shape(Point(lon, lat)) if lon and lat else None
    new_station.date_created = (
        str_strip_whitespace(entry.get("timestamp")) or datetime.now()
    )
    new_station.raw_data = json.dumps(entry, ensure_ascii=False)

    return new_station


def map_address_osm(entry: JSON, station_id: Optional[int]) -> Optional[Address]:
    """This method maps the address information from the OpenStreetMap (OSM) entry to an Address object.

    :param entry: A dictionary representing the address entry
    :param station_id: An optional integer representing the station ID
    :return: An optional Address object
    """
    tags = entry.get('tags')
    if not tags:
        return None

    if not all(key in tags for key in ADDRESS_KEYS):
        return None

    map_address = Address()
    map_address.station_id = station_id
    map_address.street = (
        str_strip_whitespace(
            str_strip_whitespace(tags.get("addr:street"))
            + " "
            + str_strip_whitespace(tags.get("addr:housenumber"))
        )
        or None
    )
    map_address.town = str_strip_whitespace(tags.get("addr:city")) or None
    map_address.postcode = str_strip_whitespace(tags.get("addr:postcode")) or None
    map_address.country = str_strip_whitespace(tags.get("addr:country")) or None

    return map_address


def _extract_ampere_list(datapoint: JSON) -> list[float]:
    raw = datapoint.get("tags", {}).get("amperage")
    if not raw:
        return []
    raw = str_clean_pattern(raw, r"(kva|ac|dc|a)")
    list_raw = str_split_pattern(raw, r"[,;\-]")

    return lst_filter_none(map(str_to_float, list_raw))


def _extract_volt_list(datapoint: JSON) -> list[float]:
    raw = datapoint.get("tags", {}).get("voltage")
    if not raw:
        return []
    if "kw" in raw.lower():
        return []
    raw = str_clean_pattern(raw, r"(v)")
    list_raw = str_split_pattern(raw, r"[,;\/ \-]")
    list_float = lst_filter_none(map(str_to_float, list_raw))

    return [float_num for float_num in list_float if float_num > 0.0]


def _extract_capacity(datapoint: JSON) -> Optional[int]:
    raw = datapoint.get('tags', {}).get('capacity')
    if not raw:
        return None

    return str_to_float(str_strip_whitespace(raw))


def extract_kw_list(raw: Optional[str]) -> list[float]:
    """Extracts a list of kW values from a raw string."""
    if not raw:
        return []
    raw_str = str_clean_pattern(raw, r"(kw|kva)")
    raw_list = str_split_pattern(raw_str, r"[,;\-]")

    return lst_filter_none(map(str_to_float, raw_list))


def extract_kw_map(datapoint: JSON) -> dict[str, list[float]]:
    """Extracts a map of socket types to kW values from a datapoint."""
    tags = datapoint.get('tags', {})

    socket_output_dict = {}
    for socket_type in SOCKET_TYPES.keys():
        socket_output_dict[socket_type] = extract_kw_list(
            tags.get(f"{socket_type}:output")
        )
    socket_output_map = {k: v for k, v in socket_output_dict.items() if v}

    return socket_output_map


def calc_total_kw(
    kw_list: Optional[list[float]],
    station_output_raw: Optional[str],
) -> Optional[float]:
    """Calculates the total output of a charging station in kW.

    The total output is the maximum value either summed output of all
    sockets or the output parsed from station raw output.

    :param kw_list: list of kw output for each socket.
    :param station_output_raw: raw station output string

    :return: maximum total output of the station
    """

    # sum output of all sockets if available, otherwise default to 0
    summed_total_kw = sum(kw_list) if kw_list else 0.0

    # Prepare station output from raw str
    cleaned_str = str_clean_pattern(station_output_raw, r"(kw,;\-)")
    striped_str = str_strip_whitespace(cleaned_str)

    # Convert raw station output str to float, otherwise default to 0
    station_output = str_to_float(striped_str) or 0.0

    # Return the maximum of the summed output and single station output
    return max(summed_total_kw, station_output)


def map_charging_osm(row: JSON, station_id: Optional[int]) -> Charging:
    """Extracts charging station data from a row of the OSM dataset.

    :param row: A dictionary containing the charging station data.
    :param station_id: The ID of the charging station.

    :return: A Charging object populated with the extracted data.
    """
    # OSM's Tag:amenity=charging_station - https://wiki.openstreetmap.org/wiki/Tag:amenity%3Dcharging_station
    kw_map = extract_kw_map(row)
    kw_list = lst_flatten([v for v in kw_map.values()])

    charging = Charging()
    charging.station_id = station_id
    charging.capacity = _extract_capacity(row) or None
    charging.kw_list = kw_list or None
    charging.ampere_list = _extract_ampere_list(row) or None
    charging.volt_list = _extract_volt_list(row) or None
    charging.socket_type_list = [SOCKET_TYPES.get(k) for k in kw_map.keys()] or None
    charging.dc_support = None
    charging.total_kw = (
        calc_total_kw(kw_list, row.get("tags", {}).get("charging_station:output"))
        or None
    )
    charging.max_kw = max(kw_list) if kw_list else None

    return charging
