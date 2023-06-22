import logging
from datetime import datetime

import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.shared import check_coordinates

logger = logging.getLogger(__name__)


def map_address_fra(row):
    street: str = row["adresse_station"]
    postcode: str = str(row["consolidated_code_postal"])
    town: str = row["consolidated_commune"]
    country: str
    # if not pd.isna(town):
    # logger.warning(f"Failed to process town {town}! Will set town to None!")
    # town = None
    map_address = Address()
    map_address.street = (street,)
    map_address.town = (town,)
    map_address.postcode = (postcode,)
    map_address.country = ("FR",)
    return map_address


def map_station_fra(row):
    lat = check_coordinates(row["consolidated_latitude"])
    long = check_coordinates(row["consolidated_longitude"])

    new_station = Station()
    datasource = "FRGOV"
    new_station.country_code = "FR"
    new_station.source_id = row["id_station_itinerance"]
    new_station.operator = row["nom_operateur"]
    new_station.data_source = datasource
    new_station.point = from_shape(Point(float(long), float(lat)))
    # new_station.date_created = (row["date_mise_en_service"].strptime("%Y-%m-%d"),)
    # new_station.date_updated = (row["date_maj"].strptime("%Y-%m-%d"),)
    if not pd.isna(row["date_mise_en_service"]):
        new_station.date_created = datetime.strptime(row["date_mise_en_service"], "%Y-%m-%d")
    if not pd.isna(row["date_maj"]):
        new_station.date_updated = datetime.strptime(row["date_maj"], "%Y-%m-%d")
    else:
        new_station.date_updated = datetime.now
    return new_station


def map_charging_fra(row):
    mapped_charging_fra: Charging = Charging()
    mapped_charging_fra.capacity = row["nbre_pdc"]

    return mapped_charging_fra
