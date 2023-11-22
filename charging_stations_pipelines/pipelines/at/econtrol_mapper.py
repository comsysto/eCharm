import logging
from typing import Optional

from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.at import DATA_SOURCE_KEY
from charging_stations_pipelines.pipelines.shared import check_coordinates

logger = logging.getLogger(__name__)


# TODO move to shared
def _clean_str(s: Optional[str]) -> Optional[str]:
    return str(s).strip() if s is not None else None


def map_station(row) -> Station:
    # TODO: check types
    lat = check_coordinates(row.get('location', {}).get('latitude'))
    long = check_coordinates(row.get('location', {}).get('longitude'))
    # Note: also available in the data source:
    #  1. status          9469 non-null
    #  2. label           9469 non-null
    #  3. description     5668 non-null
    #  4. openingHours    9469 non-null
    #  5. priceUrl        2880 non-null
    #
    #  6. evseCountryId   9469 non-null
    #  7. evseStationId   9469 non-null
    #  8. evseOperatorId  9469 non-null

    station = Station()
    station.country_code = _clean_str(row.get('evseCountryId'))  # should be always 'AT'
    station.source_id = _clean_str(row.get('evseStationId'))  # TODO ?lat_long_hash(lat, long, datasource)

    # TODO: do we have more than the ID?
    # contactName=E-Werk der Stadtgemeinde Kindberg
    # label=Ladestelle Roßdorf Platz
    # evseOperatorId=014
    station.operator = _clean_str(row.get('evseOperatorId'))

    # TODO 'points'
    #   'priceInCentPerKwh': 12,
    #   'priceInCentPerMin': 13,
    station.payment = None

    # TODO 'points'
    # authenticationModes': ['APP',
    #    'SMS',
    #    'RFID',
    #    'NFC',
    #    'DEBIT_CARD',
    #    'CASH',
    #    'CREDIT_CARD',
    #    'WEBSITE'],
    station.authentication = None

    station.data_source = DATA_SOURCE_KEY
    station.point = from_shape(Point(float(long), float(lat)))
    station.date_created = None  # Note: is not available
    station.date_updated = None  # Note: is not available

    return station


def map_address(row, station_id) -> Address:
    # Note: also available in the data source:
    #  1. contactName     9469 non-null
    #  2. telephone       8615 non-null
    #  3. email           2358 non-null
    #  4. website         4951 non-null
    address = Address()
    address.station_id = station_id
    address.street = _clean_str(row.get('street'))
    address.town = _clean_str(row.get('city')).capitalize()
    # TODO postcode does not exist in the model yet
    # TODO check type
    address.postcode = _clean_str(row.get('postCode'))
    address.district_old = None  # # Note: is not available
    address.district = None  # # Note: is not available
    address.state_old = None  # # Note: is not available
    address.state = None  # Note: is not available
    address.country = _clean_str(row.get('evseCountryId'))  # should be always 'AT'

    return address


def _sanitize_charging_attributes(charging):
    # TODO
    pass


def _extract_kw_socket_type(row) -> ([], []):
    # TODO
    kw_list = []
    socket_type_list = []

    return kw_list, socket_type_list


def map_charging(row, station_id) -> Charging:
    # kw_list from 'energyInKw'
    # socket_type_list: [connectorTypes], e.g.['CTESLA', 'CG105', 'CCCS2', 'CCCS1']
    kw_list, socket_type_list = _extract_kw_socket_type(row)

    charging = Charging()
    charging.station_id = station_id
    charging.capacity = None  # TODO number of connectors? number of charging points?
    charging.kw_list = kw_list
    charging.ampere_list = []  # NOTE: is not available
    charging.volt_list = []  # NOTE: is not available
    charging.socket_type_list = socket_type_list
    charging.dc_support = None  # NOTE: is not available
    charging.total_kw = sum(kw_list)
    charging.max_kw = max(kw_list)

    charging = _sanitize_charging_attributes(charging)

    return charging
