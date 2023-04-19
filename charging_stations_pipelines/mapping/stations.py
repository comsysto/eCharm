import logging
from datetime import datetime
import hashlib
from typing import Dict, Optional
import pandas as pd

from dateutil import parser
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.utils.bna_functions import check_coordinates
from geoalchemy2.shape import from_shape

logger = logging.getLogger(__name__)

def lat_long_hash(lat_row, long_row, data_source):
    id_hash: hashlib._Hash = hashlib.sha256(
        f"{lat_row}{long_row}{data_source}".encode("utf8")
    )
    identifier: str = id_hash.hexdigest()
    return identifier


def map_address_bna(row, station_id) -> Address:
    street: str = row["Straße"] + " " + str(row["Hausnummer"])
    postcode: str = str(row["Postleitzahl"])
    town: str = row["Ort"]
    state_old: str
    country: str
    # workaround to keep leading zero in postcode
    if len(postcode) == 4:
        postcode = "0" + postcode
    if len(postcode) != 5:
        logger.warning(
            f"Failed to process postcode {postcode}! Will set postcode to None!"
        )
        postcode = None
    if len(town) < 2:
        logger.warning(f"Failed to process town {town}! Will set town to None!")
        town = None
    map_address = Address()
    map_address.street = (street,)
    map_address.town = (town,)
    map_address.postcode = (postcode,)
    map_address.district_old = (row["Kreis/kreisfreie Stadt"],)
    map_address.state_old = (row["Bundesland"],)
    map_address.country = ("DE",)
    return map_address


def map_station_bna(row):
    lat = check_coordinates(row["Breitengrad"])
    long = check_coordinates(row["Längengrad"])

    new_station = Station()
    datasource = "BNA"
    new_station.country_code = "DE"
    new_station.source_id = lat_long_hash(lat, long, datasource)
    new_station.operator = row["Betreiber"]
    new_station.data_source = datasource
    new_station.point = from_shape(Point(float(long), float(lat)))
    new_station.date_created = (row["Inbetriebnahmedatum"].strftime("%Y-%m-%d"),)
    return new_station


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


def parse_date(date):
    try:
        return parser.parse(date)
    except TypeError as e:
        logger.debug(f"Could not parse DateCreated! {e}")
        return None


def map_station_osm(entry: Dict, country_code: str):
    datasource: str = "OSM"
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


def map_address_osm(entry, station_id):
    if "tags" in entry:
        tags = entry["tags"]
        if (
            "addr:city" in tags
            and "addr:country" in tags
            and "addr:housenumber" in tags
            and "addr:postcode" in tags
            and "addr:street" in tags
        ):
            city = tags["addr:city"]
            country = tags["addr:country"]
            housenumber = tags["addr:housenumber"]
            postcode = tags["addr:postcode"]
            street = tags["addr:street"]
            map_address = Address()
            map_address.state_old = None
            map_address.station_id = station_id
            map_address.street = street + " " + housenumber
            map_address.town = city
            map_address.postcode = postcode
            map_address.district_old = None
            map_address.country = country
            return map_address
    return None


def map_address_ocm(row, station_id):
    address_info: Dict = row.get("AddressInfo", {})
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


#functions for france gov data:
def map_address_fra(row, station_id):
    street: str = row["adresse_station"]
    postcode: str = str(row["consolidated_code_postal"])
    town: str = row["consolidated_commune"]
    country: str
    #if not pd.isna(town):
        #logger.warning(f"Failed to process town {town}! Will set town to None!")
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
    #new_station.date_created = (row["date_mise_en_service"].strptime("%Y-%m-%d"),)
    #new_station.date_updated = (row["date_maj"].strptime("%Y-%m-%d"),)
    if not pd.isna(row["date_mise_en_service"]):
        new_station.date_created = datetime.strptime(row["date_mise_en_service"], "%Y-%m-%d")
    if not pd.isna(row["date_maj"]):
        new_station.date_updated = datetime.strptime(row["date_maj"], "%Y-%m-%d")
    else: 
        new_station.date_updated = datetime.now
    return new_station



#functions for GB gov data:
def map_station_gb(entry, country_code: str):
    datasource = "GBGOV"
    lat: float = check_coordinates(entry.get("ChargeDeviceLocation").get("Latitude"))
    long: float = check_coordinates(entry.get("ChargeDeviceLocation").get("Longitude"))
    operator: Optional[str] = entry.get("DeviceController").get("OrganisationName")
    new_station = Station()
    new_station.country_code = country_code
    new_station.source_id = entry.get("ChargeDeviceId")
    new_station.operator = operator
    new_station.data_source = datasource
    new_station.point = from_shape(Point(float(long), float(lat)))
    new_station.date_created = entry.get("DateCreated")
    new_station.date_updated = entry.get("DateUpdated")
    # TODO: find way to parse date into desired format
    #parse_date having issues with "date out of range" at value 0"
    return new_station


def map_address_gb(entry, station_id):
    address_info: Dict = entry.get("ChargeDeviceLocation").get("Address", {})
    postcode_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("PostCode")
    postcode: Optional[str] = postcode_raw

    town_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("PostTown")
    town: Optional[str] = town_raw if isinstance(town_raw, str) else None

    state_old_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("County")
    state_old: Optional[str] = state_old_raw if isinstance(state_old_raw, str) else None

    country: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("Country")

    street_raw: Optional[str] = entry.get("ChargeDeviceLocation").get("Address").get("Street")
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