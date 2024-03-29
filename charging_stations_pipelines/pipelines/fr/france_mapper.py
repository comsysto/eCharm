"""Mapper for the French charging stations data."""

import logging
from datetime import datetime

import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.shared import check_coordinates

logger = logging.getLogger(__name__)


def map_address_fra(row: pd.Series) -> Address:
    """Map the address."""
    address = Address()

    address.street = row.get("adresse_station")
    address.town = row.get("consolidated_commune")
    address.postcode = row.get("consolidated_code_postal")
    address.country = "FR"

    return address


def map_station_fra(row: pd.Series) -> Station:
    """Map the station."""
    station = Station()

    station.country_code = "FR"
    station.source_id = row.get("id_station_itinerance")
    station.operator = row.get("nom_operateur")
    station.data_source = "FRGOV"
    station.point = from_shape(
        Point(
            float(check_coordinates(row.get("consolidated_longitude"))),
            float(check_coordinates(row.get("consolidated_latitude"))),
        )
    )

    if not pd.isna(row.get("date_mise_en_service")):
        station.date_created = datetime.strptime(row.get("date_mise_en_service"), "%Y-%m-%d")
    if not pd.isna(row.get("date_maj")):
        station.date_updated = datetime.strptime(row.get("date_maj"), "%Y-%m-%d")

    return station


def map_charging_fra(row) -> Station:
    """Map the charging."""
    charging = Charging()

    charging.capacity = row.get("nbre_pdc")

    return charging
