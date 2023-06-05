import logging
from typing import Optional

import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.shared import check_coordinates, parse_date

logger = logging.getLogger(__name__)


def map_station_ocm(row, country_code: str):
    datasource = "OCM"
    lat: float = check_coordinates(row["AddressInfo.Latitude"])
    long: float = check_coordinates(row["AddressInfo.Longitude"])
    operator: Optional[str] = row["Title_y"]
    new_station = Station()
    new_station.country_code = country_code
    new_station.source_id = row["ID"]
    new_station.operator = operator
    new_station.data_source = datasource
    new_station.point = from_shape(Point(float(long), float(lat)))
    new_station.date_created = parse_date(row.get("DateCreated"))
    new_station.date_updated = parse_date(row.get("DateUpdated"))
    return new_station


def map_address_ocm(row, station_id):
    postcode_raw: Optional[str] = row["AddressInfo.Postcode"]
    postcode: Optional[str] = postcode_raw

    town_raw: Optional[str] = row["AddressInfo.Town"]
    town: Optional[str] = town_raw if isinstance(town_raw, str) else None

    state_old_raw: Optional[str] = row["AddressInfo.StateOrProvince"]
    state_old: Optional[str] = state_old_raw if isinstance(state_old_raw, str) else None

    country: Optional[str] = row["Title_x"]

    street_raw: Optional[str] = row["AddressInfo.AddressLine1"]
    street: Optional[str] = street_raw if isinstance(street_raw, str) else None

    map_address = Address()
    map_address.state_old = None
    map_address.station_id = station_id
    map_address.street = street
    map_address.town = town
    map_address.postcode = postcode
    map_address.district_old = None
    map_address.state_old = state_old
    map_address.country = country
    return map_address


def map_charging_ocm(row, station_id):
    connections: pd.DataFrame = pd.DataFrame(row.get("Connections", [])).transpose()

    mapped_charging_ocm = Charging()
    mapped_charging_ocm.station_id = station_id
    mapped_charging_ocm.capacity = row.get("NumberOfPoints")
    mapped_charging_ocm.kw_list = None
    mapped_charging_ocm.ampere_list = (
        connections["Amps"].to_list() if "Amps" in connections.columns else None
    )
    mapped_charging_ocm.volt_list = (
        connections["Voltage"].to_list() if "Voltage" in connections.columns else None
    )
    mapped_charging_ocm.socket_type_list = (
        connections["Title"].str.cat(sep=",")
        if "Title" in connections.columns
        else None
    )
    mapped_charging_ocm.dc_support = None
    mapped_charging_ocm.total_kw = (
        connections["PowerKW"].dropna().sum()
        if "PowerKW" in connections.columns
        else None
    )
    mapped_charging_ocm.max_kw = (
        connections["PowerKW"].dropna().max()
        if "PowerKW" in connections.columns
        else None
    )

    return mapped_charging_ocm
