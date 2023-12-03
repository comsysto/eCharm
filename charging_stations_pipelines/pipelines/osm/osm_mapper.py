import logging
import math
import math
from datetime import datetime
from typing import Dict, List, Optional

from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.osm import DATA_SOURCE_KEY
from charging_stations_pipelines.pipelines.shared import check_coordinates
from charging_stations_pipelines.shared import filter_none, try_float, try_clean_str, try_split_str

logger = logging.getLogger(__name__)


def map_station_osm(entry: Dict, country_code: str) -> Station:
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


def map_address_osm(entry: Dict, station_id: Optional[int]) -> Optional[Address]:
    """This method maps the address information from the OpenStreetMap (OSM) entry to an Address object.

    :param entry: A dictionary representing the address entry
    :param station_id: An optional integer representing the station ID
    :return: An optional Address object
    """
    tags = entry.get('tags')
    if not tags:
        return None
    address_keys = [  # Final[list[LiteralString]]
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
        # TODO add state_old to Address
        # map_address.state_old = None
        map_address.station_id = station_id
        map_address.street = street + " " + housenumber
        map_address.town = city
        # TODO add postcode to Address
        # map_address.postcode = postcode
        map_address.district_old = None
        map_address.country = country

        return map_address


def _extract_ampere_list(datapoint: Dict[str, Optional[str]]) -> List[float]:
    raw = datapoint.get('tags', {}).get('amperage')
    if not raw:
        return []
    raw = try_clean_str(raw, r'(kva|ac|dc|a)')
    list_raw = try_split_str(raw, '[,;-]')
    return filter_none(map(try_float, list_raw))


def _extract_volt_list(datapoint: Dict[str, Optional[str]]) -> List[float]:
    raw = datapoint.get('tags', {}).get('voltage')
    if not raw:
        return []
    if 'kw' in raw.lower():
        return []
    raw = try_clean_str(raw, r'(v)')
    list_raw = try_split_str(raw, r'[,;\\/ \\-]')
    return filter_none(map(lambda v: math.trunc(v) if v else None, map(try_float, list_raw)))


def _extract_capacity(datapoint: Dict[str, Optional[str]]) -> Optional[int]:
    raw = datapoint.get('tags', {}).get('capacity')
    if not raw:
        return None
    capacity_raw = try_float(raw.strip())
    return math.trunc(capacity_raw) if capacity_raw else None


def _extract_socket_type_list(datapoint: Dict[str, Optional[str]]) -> List[str]:
    # +---------------------------------------------------------------------------------------+
    # |socket_type_list                                                                       |
    # +---------------------------------------------------------------------------------------+
    # |null                                                                                   |
    # |"{}"                                                                                   |
    # |"{"AC Steckdose Typ 2","AC Steckdose Typ 2"}"                                          |
    # |"{"AC Steckdose Typ 2"}"                                                               |
    # |"{"DC Kupplung Combo","DC Kupplung Combo"}"                                            |
    # |"{"AC Steckdose Typ 2"," AC Schuko","AC Steckdose Typ 2"," AC Schuko"}"                |
    # |"{"DC Kupplung Combo","AC Steckdose Typ 2"}"                                           |
    # |"{2}"                                                                                  |
    # |"{"AC Steckdose Typ 2","AC Steckdose Typ 2","AC Steckdose Typ 2","AC Steckdose Typ 2"}"|
    # |"{"AC Kupplung Typ 2","AC Kupplung Typ 2"}"                                            |
    # |"{"AC Kupplung Typ 2","DC Kupplung Combo"," DC CHAdeMO"}"                              |
    # |"{"AC Kupplung Typ 2"}"                                                                |
    # |"{"DC Kupplung Combo","DC Kupplung Combo","AC Steckdose Typ 2"}"                       |
    # |"{"AC Steckdose Typ 2"," AC Kupplung Typ 2","AC Steckdose Typ 2"," AC Kupplung Typ 2"}"|
    # |"{1}"                                                                                  |
    # |"{"DC Kupplung Combo"," DC CHAdeMO","AC Steckdose Typ 2"}"                             |
    # |"{4}"                                                                                  |
    # |"{"AC Steckdose Typ 2","DC Kupplung Combo"}"                                           |
    # |"{"DC Kupplung Combo"," DC CHAdeMO","AC Kupplung Typ 2"}"                              |
    # |"{"DC Kupplung Combo"}"                                                                |
    # +---------------------------------------------------------------------------------------+
    socket_types = [('socket:chademo', 'CHAdeMO'),  # Final[list[tuple[LiteralString, LiteralString]]]
                    ('socket:schuko', 'AC Schuko'),
                    ('socket:tesla_supercharger', 'Tesla Supercharger'),
                    ('socket:type2', 'AC Steckdose Typ 2'),
                    ('socket:type2_combo', 'AC Steckdose Typ 2 Combo'),
                    ('socket:type3', 'AC Steckdose Typ 3'),
                    ('socket:type3c', 'AC Steckdose Typ 3c'),
                    ('socket:typee', 'AC Steckdose Typ E')]

    socket_type_list = []
    for socket_type_id, socket_type_name in socket_types:
        raw = datapoint.get('tags', {}).get(socket_type_id)

        if not raw:
            continue

        raw = raw.lower()
        if 'no' in raw:
            continue

        if 'yes' in raw or any(char.isdigit() for char in raw):
            socket_type_list.append(socket_type_name)

    return socket_type_list


def _extract_kw_list(datapoint: Dict[str, Optional[str]]) -> List[float]:
    # TODO: consider all other fields
    raw = datapoint.get('tags', {}).get('socket:output')
    if not raw:
        return []
    raw = try_clean_str(raw, r'(kw|kva)')
    list_raw = try_split_str(raw, '[,;-]')
    return filter_none(map(try_float, list_raw))


def _calc_total_kw(kwlist: List[Optional[float]]) -> Optional[float]:
    return sum(kwlist) if kwlist else None


def _calc_max_kw(kwlist: List[Optional[float]]) -> Optional[float]:
    return max(kwlist) if kwlist else None


def _extract_dc_support(datapoint: Dict[str, Optional[str]]) -> Optional[bool]:
    # TODO
    return False


def _sanitize_attributes(final_charging: Charging) -> Charging:
    # max sockets/charging points per charging station
    max_capacity = 4  # Final[int]
    # # average capacity across all charging stations is set when capacity>MAX_CAPACITY
    avg_capacity = 2  # Final[int]
    if final_charging.capacity and final_charging.capacity > max_capacity:
        final_charging.capacity = avg_capacity

    return final_charging


def map_charging_osm(row: Dict[str, Optional[str]], station_id: Optional[int]) -> Charging:
    """Extracts charging station data from a row of the OSM dataset.

    :param row: A dictionary containing the charging station data.
    :param station_id: The ID of the charging station.

    :return: A Charging object populated with the extracted data.
    """
    # OSM's Tag:amenity=charging_station - https://wiki.openstreetmap.org/wiki/Tag:amenity%3Dcharging_station
    charging = Charging()
    charging.station_id = station_id
    charging.capacity = _extract_capacity(row)
    charging.kw_list = _extract_kw_list(row)
    charging.ampere_list = _extract_ampere_list(row)
    charging.volt_list = _extract_volt_list(row)
    charging.socket_type_list = _extract_socket_type_list(row)
    charging.dc_support = _extract_dc_support(row)
    charging.total_kw = _calc_total_kw(charging.kw_list)
    charging.max_kw = _calc_max_kw(charging.kw_list)

    charging = _sanitize_attributes(charging)

    return charging
