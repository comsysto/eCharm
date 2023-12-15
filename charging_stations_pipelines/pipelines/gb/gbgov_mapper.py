"""Mapper for the GBGOV data source."""

import logging
from typing import Optional

from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.shared import check_coordinates, JSON
from . import DATA_SOURCE_KEY

logger = logging.getLogger(__name__)


def map_station_gb(entry: JSON, country_code: str) -> Station:
    """Maps the station data from the GBGOV data source."""
    lat: float = check_coordinates(entry.get("ChargeDeviceLocation").get("Latitude"))
    long: float = check_coordinates(entry.get("ChargeDeviceLocation").get("Longitude"))
    operator: Optional[str] = entry.get("DeviceController").get("OrganisationName")

    station = Station()
    station.country_code = country_code
    station.source_id = entry.get("ChargeDeviceId")
    station.operator = operator
    station.data_source = DATA_SOURCE_KEY
    station.point = from_shape(Point(float(long), float(lat)))
    station.date_created = entry.get("DateCreated")
    station.date_updated = entry.get("DateUpdated")

    # TODO: find way to parse date into desired format
    # FIXME: parse_date having issues with "date out of range" at value 0"

    return station


def map_address_gb(entry: JSON, station_id: Optional[int]) -> Address:
    """Maps the address data from the GBGOV data source."""
    postcode_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("PostCode")
    postcode: Optional[str] = postcode_raw

    town_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("PostTown")
    town: Optional[str] = town_raw if isinstance(town_raw, str) else None

    state_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("County")
    state: Optional[str] = state_raw if isinstance(state_raw, str) else None

    country: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("Country")

    street_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("Street")
    street: Optional[str] = street_raw if isinstance(street_raw, str) else None

    address = Address()
    address.state = None
    address.station_id = station_id
    address.street = street
    address.town = town
    address.postcode = postcode
    address.district = None
    address.state = state
    address.country = country

    return address


def map_charging_gb(entry: JSON) -> Charging:
    """Maps the charging data from the GBGOV data source."""
    charging: Charging = Charging()

    charging.capacity = entry.get("RatedOutputCurrent")

    # FIXME: above is not correct information (just there for testing purposes)
    # TODO:find way to count the points of charge for each station
    # since this information is not available in the json

    return charging
