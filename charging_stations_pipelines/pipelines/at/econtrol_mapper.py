import logging
from typing import Final, Any, Optional

from geoalchemy2 import WKBElement
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.at import DATA_SOURCE_KEY
from charging_stations_pipelines.pipelines.shared import check_coordinates, try_strip_str, try_flatten_list, try_float, \
    try_expand_list, try_remove_dups

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


def _extract_charger_details(points: list[dict]) -> ([], [], []):
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


def _extract_location(row) -> WKBElement:
    location = row.get('location', {})  # Final[dict]
    lon = check_coordinates(location.get('longitude'))  # Final[float]
    lat = check_coordinates(location.get('latitude'))  # Final[float]
    return from_shape(Point(lon, lat))


def map_station(row) -> Station:
    # Note: following attributes are also available in the data source
    #  1. status          9469 non-null
    #  2. label           9469 non-null
    #  3. description     5668 non-null
    #  4. openingHours    9469 non-null
    #  5. priceUrl        2880 non-null
    #  ...
    #  6. evseCountryId   9469 non-null
    #  7. evseStationId   9469 non-null
    #  8. evseOperatorId  9469 non-null

    station: Station = Station()

    station.country_code = try_strip_str(row.get('evseCountryId'))  # should be always 'AT'
    station.source_id = try_strip_str(row.get('evseStationId'))

    # TODO there are also other attributes available in the data source,
    #  which could be useful for the `operator`, e.g.:
    # 1. `contactName`, e.g.: "E-Werk der Stadtgemeinde Kindberg"
    # 2. `label`, e.g.: "Ladestelle Roßdorf Platz"
    station.operator = try_strip_str(row.get('evseOperatorId'))  # evseOperatorId, e.g. "014"

    station.payment = None  # TODO check semantics
    station.authentication = _extract_charger_details(row)  # TODO check semantics
    station.data_source = DATA_SOURCE_KEY
    station.point = _extract_location(row)
    station.date_created = None  # Note: is not available in the data source
    station.date_updated = None  # Note: is not available in the data source

    return station


def map_address(row, station_id) -> Address:
    # Note: following attributes are also available in the data source:
    #  1. contactName     9469 non-null
    #  2. telephone       8615 non-null
    #  3. email           2358 non-null
    #  4. website         4951 non-null
    address: Address = Address()

    address.station_id = station_id
    address.street = try_strip_str(row.get('street'))
    address.town = try_strip_str(row.get('city')).capitalize()
    address.postcode = try_strip_str(row.get('postCode'))
    address.district_old = None  # # Note: is not available in the data source
    address.district = None  # # Note: is not available in the data source
    address.state_old = None  # # Note: is not available in the data source
    address.state = None  # Note: is not available in the data source
    address.country = try_strip_str(row.get('evseCountryId'))  # should be always 'AT'

    return address


def _sanitize_charging_attributes(final_charging: Charging) -> Charging:
    # max sockets/charging points per charging station
    max_capacity = 4  # Final[int]
    # # average capacity across all charging stations is set when capacity>MAX_CAPACITY
    avg_capacity = 2  # Final[int]
    if final_charging.capacity is not None and final_charging.capacity > max_capacity:
        final_charging.capacity = avg_capacity

    return final_charging


def map_charging(row, station_id) -> Charging:
    kw_list, socket_type_list = _extract_charger_details(row)

    charging: Charging = Charging()
    charging.station_id = station_id
    charging.capacity = len(kw_list)  # TODO number of connectors? number of charging points? check semantics
    charging.kw_list = kw_list
    charging.ampere_list = []  # NOTE: is not available in the data source
    charging.volt_list = []  # NOTE: is not available in the data source
    charging.socket_type_list = socket_type_list
    charging.dc_support = None  # NOTE: is not available in the data source
    charging.total_kw = sum(kw_list)
    charging.max_kw = max(kw_list)

    charging = _sanitize_charging_attributes(charging)

    return charging
