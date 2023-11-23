"""Module to extract and map the raw data from the datasource to internal DB data model."""

import logging
from typing import Final, Any, Optional

from geoalchemy2 import WKBElement
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.at import DATA_SOURCE_KEY
from charging_stations_pipelines.pipelines.shared import check_coordinates, try_strip_str, try_flatten_list, \
    try_float, try_expand_list, try_remove_dups

logger = logging.getLogger(__name__)


def _aggregate_attribute(points: list[dict], attr: str, process_func=None) -> list[Any]:
    attr_list_agg = []  # type: Final[list[Any]]
    for p in points:
        attr_vals = p.get(attr, [])  # type: list[Any]
        attr_list_agg.append(attr_vals)
    attr_list = attr_list_agg if process_func is None else process_func(attr_list_agg)  # type: Final[list[Any]]
    return attr_list


def _extract_auth_modes(points: list[dict]) -> list[str]:
    # Aggregate authentication modes
    auth_modes_agg = _aggregate_attribute(points, 'authenticationModes')  # type: Final[list[list[str]]]
    flattened_auth_modes_agg = try_flatten_list(auth_modes_agg)  # type: Final[list[str]]
    auth_modes_list = try_remove_dups(flattened_auth_modes_agg)  # type: Final[list[str]]

    return auth_modes_list


def _extract_location(row) -> Optional[WKBElement]:
    location = row.get('location', {})  # type: Final[dict]
    lon = check_coordinates(location.get('longitude'))  # type: Final[float]
    lat = check_coordinates(location.get('latitude'))  # type: Final[float]
    return from_shape(Point(lon, lat)) if lon and lat else None


def map_station(row: dict[str, str]) -> Station:
    """
    :param row: A dictionary representing the raw datapoint of a station.
    :return: A Station object.

    This method maps the data from the given row dictionary to create a Station object. \
    The row dictionary should have the following attributes:

    - 'points': A list of dictionaries representing the charging points of the station.

    The Station object will have the following attributes:

    - 'country_code': A string representing the country code of the station. It should always be 'AT'.
    - 'source_id': A string representing the source ID of the station.
    - 'operator': A string representing the operator of the station.
    - 'payment': The payment information for the station is not available in the data source.
    - 'authentication': A list of authentication modes supported by the station, extracted from the 'points' \
      attribute of the row dictionary.
    - 'data_source': A string representing the data source key.
    - 'point': GPS coordinates of the station.
    - 'raw_data': The raw data of the station.
    - 'date_created': The creation date of the station.
    - 'date_updated': The update date of the station.
    """
    # TODO following attributes are also available in the data source
    #  1. status          9469 non-null
    #  2. label           9469 non-null
    #  3. description     5668 non-null
    #  4. openingHours    9469 non-null
    #  5. priceUrl        2880 non-null
    #  ...
    #  6. evseCountryId   9469 non-null
    #  7. evseStationId   9469 non-null
    #  8. evseOperatorId  9469 non-null
    points = row.get('points', []) or []  # type: Final[list[dict]]

    station: Station = Station()

    station.country_code = try_strip_str(row.get('evseCountryId'))  # should be always 'AT'
    station.source_id = try_strip_str(row.get('evseStationId'))

    # TODO there are also other attributes available in the data source,
    #  which could be useful for the `operator`, e.g.:
    # 1. `contactName`, e.g.: "E-Werk der Stadtgemeinde Kindberg"
    # 2. `label`, e.g.: "Ladestelle Roßdorf Platz"
    station.operator = try_strip_str(row.get('evseOperatorId'))  # evseOperatorId, e.g. "014"

    station.payment = None  # TODO check semantics
    station.authentication = _extract_auth_modes(points)  # TODO check semantics
    station.data_source = DATA_SOURCE_KEY
    station.point = _extract_location(row)
    station.raw_data = None
    station.date_created = None  # Note: is not available in the data source
    station.date_updated = None  # Note: is not available in the data source

    return station


def map_address(row: dict[str, str], station_id: int) -> Address:
    """Maps the given raw datapoint to an Address object.

    :param row: A dictionary representing the row data.
    :param station_id: An integer representing the station ID.
    :return: An Address object.
    """
    # TODO following attributes are also available in the data source:
    #  1. contactName     9469 non-null
    #  2. telephone       8615 non-null
    #  3. email           2358 non-null
    #  4. website         4951 non-null
    address: Address = Address()

    address.station_id = station_id
    address.street = try_strip_str(row.get('street'))
    address.town = try_strip_str(row.get('city'))
    address.postcode = try_strip_str(row.get('postCode'))
    address.district_old = None  # # Note: is not available in the data source
    address.district = None  # # Note: is not available in the data source
    address.state_old = None  # # Note: is not available in the data source
    address.state = None  # Note: is not available in the data source
    address.country = try_strip_str(row.get('evseCountryId'))  # should be always 'AT'

    return address


def _extract_charger_details(points: list[dict]) -> ([], []):
    # Aggregate socket types
    socket_type_list_agg = _aggregate_attribute(points, 'connectorTypes')  # type: Final[list[list[str]]]
    socket_type_list = try_flatten_list(socket_type_list_agg)  # type: Final[list[str]]

    # Aggregate energy_in_kw
    kw_list_agg = []  # type: Final[list[tuple[float, int]]]
    for p in points:
        energy_in_kw = try_float(p.get('energyInKw'))  # type: Optional[float]
        kw_list_agg.append((energy_in_kw, len(p.get('connectorTypes', []))))
    kw_list = try_expand_list(kw_list_agg)  # type: Final[list[float]]

    return kw_list, socket_type_list


def _sanitize_charging_attributes(final_charging: Charging) -> Charging:
    # max sockets/charging points per charging station
    max_capacity = 4  # type: Final[int]
    # average capacity across all charging stations is set when capacity>MAX_CAPACITY
    avg_capacity = 2  # type: Final[int]
    if final_charging.capacity is not None and final_charging.capacity > max_capacity:
        final_charging.capacity = avg_capacity

    return final_charging


def map_charging(row: dict[str, str], station_id: int) -> Charging:
    """Maps the given raw datapoint to a Charging object.

    :param row: A dictionary containing information about the charging station.
    :param station_id: The ID of the charging station.
    :return: An instance of the Charging class containing the mapped charging point data.
    """
    points = row.get('points', []) or []  # type: Final[list[dict]]

    kw_list, socket_type_list = _extract_charger_details(points)  # type: Final[(list[float], list[str])]

    charging: Charging = Charging()
    charging.station_id = station_id
    charging.capacity = len(points)
    charging.kw_list = kw_list
    charging.ampere_list = None  # NOTE: is not available in the data source
    charging.volt_list = None  # NOTE: is not available in the data source
    charging.socket_type_list = socket_type_list
    charging.dc_support = None  # NOTE: is not available in the data source
    charging.total_kw = sum(kw_list)
    charging.max_kw = max(kw_list)

    charging = _sanitize_charging_attributes(charging)

    return charging
