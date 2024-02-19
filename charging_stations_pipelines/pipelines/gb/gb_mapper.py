import logging
from typing import Optional

from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.shared import check_coordinates, parse_date

logger = logging.getLogger(__name__)


# functions for GB gov data:
def map_station_gb(entry):
    datasource = "GBGOV"
    lat: float = check_coordinates(entry.get("ChargeDeviceLocation").get("Latitude"))
    long: float = check_coordinates(entry.get("ChargeDeviceLocation").get("Longitude"))
    new_station = Station()
    new_station.country_code = "GB"
    new_station.source_id = entry.get("ChargeDeviceId")
    new_station.operator = entry.get("DeviceController").get("OrganisationName")
    new_station.data_source = datasource
    new_station.point = from_shape(Point(float(long), float(lat)))
    new_station.date_created = parse_date(entry.get("DateCreated"))
    new_station.date_updated = parse_date(entry.get("DateUpdated"))
    return new_station


def map_address_gb(entry, station_id):
    postcode_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("PostCode")
    postcode: Optional[str] = postcode_raw

    town_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("PostTown")
    town: Optional[str] = town_raw if isinstance(town_raw, str) else None

    state_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("County")
    state: Optional[str] = state_raw if isinstance(state_raw, str) else None

    country: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("Country")

    street_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("Street")
    street: Optional[str] = street_raw if isinstance(street_raw, str) else None

    map_address = Address()
    map_address.state = None
    map_address.station_id = station_id
    map_address.street = street
    map_address.town = town
    map_address.postcode = postcode
    map_address.district = None
    map_address.state = state
    map_address.country = country
    return map_address


def map_charging_gb(entry):
    mapped_charging_gb: Charging = Charging()
    mapped_charging_gb.capacity = entry.get("RatedOutputCurrent")
    # above is not correct information (just there for testing purposes)
    # TODO:find way to count the points of charge for each station
    # since this information is not available in the json

    return mapped_charging_gb
