import datetime
import hashlib
from typing import Dict, Optional

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
    lat = check_coordinates(row["AddressInfo.Latitude"])
    long = check_coordinates(row["AddressInfo.Longitude"])
    operator: Optional[str] = row["Title"]
    new_station = Station()
    new_station.source_id = lat_long_hash(lat, long, datasource)
    new_station.operator = operator
    new_station.data_source = datasource
    coordinates = Point(float(lat), float(long)).wkt
    new_station.coordinates = coordinates
    new_station.date_created = row["DateCreated"]
    return new_station


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
    try:
        country: Optional[Dict] = row["Title_y"]
    except:
        country = None
    try:
        postcode: Optional[str] = row["AddressInfo.Postcode"]
        postcode = (
            "".join([s for s in postcode if s.isdigit()])
            if postcode is not None
            else ""
        )
    except:
        postcode = None
    town: Optional[str] = row["AddressInfo.Town"]
    if not isinstance(town, str):
        town = ""
    state_old: Optional[str] = row["AddressInfo.StateOrProvince"]
    if state_old is None:
        state_old = ""
    country: Optional[str] = row["ISOCode"]
    street: Optional[str] = row["AddressInfo.AddressLine1"]
    if (postcode is None) or len(postcode) != 5:
        log.warning(
            f"Postcode {postcode} of town {town} is not of length 5! Will set postcode to None!"
        )
        postcode = None
    if (len(town) < 2) or (not all(not s.isdigit() for s in town)):
        log.warning(
            f"Town {town} has less than 2 chars or contains digits! Will set town to None!"
        )
        town = None
    try:
        if (not all(not s.isdigit() for s in state_old)) | (len(state_old) < 2):
            log.warning(
                f"StateOld {state_old} contains digits or has less than 2 chars! Will set state_old to None!"
            )
    except:
        state_old = None
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
