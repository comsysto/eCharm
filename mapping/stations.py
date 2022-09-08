import datetime
import hashlib
from typing import Dict, Optional

from dateutil import parser
from shapely.geometry import Point

from models.address import Address
from models.station import Station
from utils.bna_functions import check_coordinates
from utils.logging_utils import log


def lat_long_hash(lat_row, long_row, data_source):
    id_hash: hashlib._Hash = hashlib.sha256(
        f"{lat_row}{long_row}{data_source}".encode("utf8")
    )
    identifier: str = id_hash.hexdigest()
    return identifier


def map_address_bna(row, station_id):
    street: str = row["Straße"] + " " + str(row["Hausnummer"])
    postcode: str = str(row["Postleitzahl"])
    town: str = row["Ort"]
    state_old: str
    country: str
    # workaround to keep leading zero in postcode
    if len(postcode) == 4:
        postcode = "0" + postcode
    if len(postcode) != 5:
        log.warning(
            f"Failed to process postcode {postcode}! Will set postcode to None!"
        )
        postcode = None
    if len(town) < 2:
        log.warning(f"Failed to process town {town}! Will set town to None!")
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
    lat = check_coordinates(row["Längengrad"])
    long = check_coordinates(row["Breitengrad"])

    new_station = Station()
    datasource = "BNA"
    new_station.source_id = lat_long_hash(lat, long, datasource)
    new_station.operator = row["Betreiber"]
    new_station.data_source = datasource
    coordinates = Point(float(lat), float(long)).wkt
    new_station.coordinates = coordinates
    new_station.date_created = (row["Inbetriebnahmedatum"].strftime("%Y-%m-%d"),)
    return new_station


def map_station_ocm(row):
    datasource = "OCM"
    address_info: Dict = row.get("AddressInfo", {})
    lat: float = check_coordinates(row["AddressInfo.Latitude"])
    long: float = check_coordinates(row["AddressInfo.Longitude"])
    operator: Optional[str] = row["Title_y"]
    new_station = Station()
    new_station.source_id = lat_long_hash(lat, long, datasource)
    new_station.operator = operator
    new_station.data_source = datasource
    coordinates = Point(float(lat), float(long)).wkt
    new_station.coordinates = coordinates
    new_station.date_created = parse_date(row.get("DateCreated"))
    new_station.date_updated = parse_date(row.get("DateUpdated"))
    return new_station


def parse_date(date):
    try:
        return parser.parse(date)
    except TypeError as e:
        log.debug(f"Could not parse DateCreated! {e}")
        return None


def map_station_osm(entry: Dict):
    datasource: str = "OSM"
    lat = check_coordinates(entry["lat"])
    lon = check_coordinates(entry["lon"])
    operator: Optional[str] = entry["tags"].get("operator")
    new_station = Station()
    new_station.source_id = lat_long_hash(lat, lon, datasource)
    new_station.operator = operator
    new_station.data_source = datasource
    new_station.coordinates = Point(float(lat), float(lon)).wkt
    new_station.date_created = entry.get("timestamp", datetime.datetime.now())
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
