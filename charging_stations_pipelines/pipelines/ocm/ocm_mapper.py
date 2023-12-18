"""Mapper for OpenChargeMap (OCM) data."""

import logging
from typing import Optional

import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.ocm import DATA_SOURCE_KEY
from charging_stations_pipelines.shared import (
    check_coordinates,
    coalesce, JSON, parse_date,
    str_strip_whitespace,
)

logger = logging.getLogger(__name__)


def map_station_ocm(row: JSON, country_code: str) -> Station:
    """Maps an entry from OpenChargeMap to a Station object."""
    datasource = DATA_SOURCE_KEY
    lat: float = check_coordinates(row["AddressInfo.Latitude"])
    long: float = check_coordinates(row["AddressInfo.Longitude"])
    operator: Optional[str] = row["Title_y"]

    station = Station()
    station.country_code = country_code
    station.source_id = row["ID"]
    station.operator = operator
    station.data_source = datasource
    station.point = from_shape(Point(float(long), float(lat)))
    station.date_created = parse_date(row.get("DateCreated"))
    station.date_updated = parse_date(row.get("DateUpdated"))

    return station


def map_address_ocm(row: JSON, country_code: str, station_id: Optional[int]) -> Address:
    """Maps an entry from OpenChargeMap to an Address object."""
    address = Address()
    address.station_id = station_id
    address.street = str_strip_whitespace(row.get("AddressInfo.AddressLine1")) or None
    address.town = str_strip_whitespace(row.get("AddressInfo.Town")) or None
    address.postcode = str_strip_whitespace(row.get("AddressInfo.Postcode")) or None
    address.district = None
    address.state = str_strip_whitespace(row.get("AddressInfo.StateOrProvince")) or None
    address.country = coalesce(str_strip_whitespace(row.get("Title_x")), country_code)

    return address


def map_charging_ocm(row: JSON, station_id: Optional[int]) -> Charging:
    """Maps an entry from OpenChargeMap to a Charging object."""
    connections: pd.DataFrame = pd.DataFrame(row.get("Connections", []))

    charging = Charging()
    charging.station_id = station_id
    charging.capacity = row.get("NumberOfPoints")
    charging.kw_list = None
    charging.ampere_list = (
        connections["Amps"].to_list() if "Amps" in connections.columns else None
    )
    charging.volt_list = (
        connections["Voltage"].to_list() if "Voltage" in connections.columns else None
    )
    charging.socket_type_list = (
        connections["Title"].str.cat(sep=",")
        if "Title" in connections.columns
        else None
    )
    charging.dc_support = None
    charging.total_kw = (
        float(round(connections["PowerKW"].dropna().sum(), 2))
        if "PowerKW" in connections.columns
        else None
    )
    charging.max_kw = (
        float(connections["PowerKW"].dropna().max())
        if "PowerKW" in connections.columns
        else None
    )

    return charging
