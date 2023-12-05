import logging
import math
from datetime import datetime
from typing import Any, Final, Optional

from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.osm import DATA_SOURCE_KEY
from charging_stations_pipelines.pipelines.shared import check_coordinates, lst_flatten
from charging_stations_pipelines.shared import filter_none, try_clean_str, try_float, try_split_str

logger = logging.getLogger(__name__)

SOCKET_TYPES = {  # type: Final[dict[str, str]]
    'socket:chademo':            'CHAdeMO',
    'socket:schuko':             'AC Schuko',
    'socket:tesla_supercharger': 'Tesla Supercharger',
    'socket:type2':              'AC Steckdose Typ 2',
    'socket:type2:combo':        'AC Steckdose Typ 2 Combo',
    'socket:type2_combo':        'AC Steckdose Typ 2 Combo',
    'socket:type3':              'AC Steckdose Typ 3',
    'socket:type3c':             'AC Steckdose Typ 3c',
    'socket:typee':              'AC Steckdose Typ E'}


def map_station_osm(entry: dict, country_code: str) -> Station:
    """Maps an entry from OpenStreetMap to a Station object.

    :param entry: The entry from OpenStreetMap to be mapped.
    :param country_code: The country code of the station.
    :return: The mapped Station object.
    """
    datasource: str = DATA_SOURCE_KEY
    lat = check_coordinates(entry["lat"])
    lon = check_coordinates(entry["lon"])
    operator: Optional[str] = entry["tags"].get("operator")

    new_station = Station()
    new_station.country_code = country_code
    new_station.source_id = entry["id"]
    new_station.operator = operator
    new_station.data_source = datasource
    new_station.point = from_shape(Point(float(lon), float(lat)))
    new_station.date_created = entry.get("timestamp", datetime.now())
    return new_station


def map_address_osm(entry: dict, station_id: Optional[int]) -> Optional[Address]:
    """This method maps the address information from the OpenStreetMap (OSM) entry to an Address object.

    :param entry: A dictionary representing the address entry
    :param station_id: An optional integer representing the station ID
    :return: An optional Address object
    """
    tags = entry.get('tags')
    if not tags:
        return None
    address_keys = [  # type: Final[list[str]]
        "addr:city",
        "addr:country",
        "addr:housenumber",
        "addr:postcode",
        "addr:street"]
    if all(key in tags for key in address_keys):
        city = tags["addr:city"]
        country = tags["addr:country"]
        housenumber = tags["addr:housenumber"]
        postcode = tags["addr:postcode"]
        street = tags["addr:street"]

        map_address = Address()
        map_address.station_id = station_id
        map_address.street = street + " " + housenumber
        map_address.town = city
        map_address.postcode = postcode
        map_address.country = country

        return map_address


def _extract_ampere_list(datapoint: dict[str, Any]) -> list[float]:
    raw = datapoint.get('tags', {}).get('amperage')
    if not raw:
        return []
    raw = try_clean_str(raw, r'(kva|ac|dc|a)')
    list_raw = try_split_str(raw, '[,;-]')
    return filter_none(map(try_float, list_raw))


def _extract_volt_list(datapoint: dict[str, Any]) -> list[float]:
    raw = datapoint.get('tags', {}).get('voltage')
    if not raw:
        return []
    if 'kw' in raw.lower():
        return []
    raw = try_clean_str(raw, r'(v)')
    list_raw = try_split_str(raw, r'[,;\\/ \\-]')
    return filter_none(map(lambda v: math.trunc(v) if v else None, map(try_float, list_raw)))


def _extract_capacity(datapoint: dict[str, Optional[str]]) -> Optional[int]:
    raw = datapoint.get('tags', {}).get('capacity')
    if not raw:
        return None
    capacity_raw = try_float(raw.strip())
    return math.trunc(capacity_raw) if capacity_raw else None


def _extract_kw_list(datapoint: dict[str, Optional[str]]) -> list[float]:
    raw = datapoint.get('tags', {}).get('socket:output')
    if not raw:
        return []
    raw = try_clean_str(raw, r'(kw|kva)')
    list_raw = try_split_str(raw, '[,;-]')
    return filter_none(map(try_float, list_raw))


def _ext_kw_list(raw: str) -> list[float]:
    if not raw:
        return []

    raw_str = try_clean_str(raw, r'(kw|kva)')
    raw_list = try_split_str(raw_str, '[,;-]')

    return filter_none(map(try_float, raw_list))


def _extract_kw_map(datapoint: dict[str, Any]) -> dict[str, list[float]]:
    tags = datapoint.get('tags', {})

    socket_output_map: dict[str, list[float]] = {}
    for k in SOCKET_TYPES.keys():
        kw_list = _ext_kw_list(tags.get(f'{k}:output'))
        if kw_list:
            socket_output_map[k] = kw_list

    return socket_output_map


def map_charging_osm(row: dict[str, Any], station_id: Optional[int]) -> Charging:
    """Extracts charging station data from a row of the OSM dataset.

    :param row: A dictionary containing the charging station data.
    :param station_id: The ID of the charging station.

    :return: A Charging object populated with the extracted data.
    """
    # OSM's Tag:amenity=charging_station - https://wiki.openstreetmap.org/wiki/Tag:amenity%3Dcharging_station
    kw_map = _extract_kw_map(row)
    charging = Charging()
    charging.station_id = station_id
    charging.capacity = _extract_capacity(row)
    charging.kw_list = lst_flatten([v for v in kw_map.values()])
    charging.ampere_list = _extract_ampere_list(row)
    charging.volt_list = _extract_volt_list(row)
    charging.socket_type_list = [SOCKET_TYPES.get(k) for k in kw_map.keys()]
    charging.dc_support = None
    charging.total_kw = sum(charging.kw_list) if charging.kw_list else None
    charging.max_kw = max(charging.kw_list) if charging.kw_list else None

    return charging
