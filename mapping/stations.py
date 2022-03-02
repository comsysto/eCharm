import hashlib
from typing import Optional, Dict

from shapely.geometry import Point
from models.station import Station
from models.address import Address
from utils.bna_functions import check_coordinates
from utils.logging_utils import log


def lat_long_hash(lat_row, long_row):
    id_hash: hashlib._Hash = hashlib.sha256(f"{lat_row}{long_row}".encode("utf8"))
    identifier: bytes = id_hash.hexdigest().encode(
        "utf8"
    )  # TODO: should we return a string here?
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
    map_address.station_id = (station_id,)
    map_address.street = (street,)
    map_address.town = (town,)
    map_address.postcode = (postcode,)
    map_address.district_old = (row["Kreis/kreisfreie Stadt"],)
    map_address.state_old = (row["Bundesland"],)
    map_address.country = ("DE",)
    return map_address


def map_stations_bna(row):
    lat = check_coordinates(row["Längengrad"])
    long = check_coordinates(row["Breitengrad"])

    new_station = Station()
    new_station.source_id = lat_long_hash(lat, long)
    new_station.operator = row["Betreiber"]
    new_station.data_source = "BNA"
    coordinates = Point(float(lat), float(long)).wkt
    new_station.coordinates = coordinates
    new_station.date_created = (row["Inbetriebnahmedatum"].strftime("%Y-%m-%d"),)
    return new_station


def map_stations_ocm(row):
    lat = check_coordinates(row["AddressInfo.Latitude"])
    long = check_coordinates(row["AddressInfo.Longitude"])
    operator: Optional[str] = (
        row["Title"]
    )
    new_station = Station()
    new_station.source_id = lat_long_hash(lat, long)
    new_station.operator = operator
    new_station.data_source = "OCM"
    coordinates = Point(
        float(lat),
        float(long)
    ).wkt
    new_station.coordinates = coordinates
    new_station.date_created = row["DateCreated"]
    return new_station


def map_address_ocm(row, station_id):
    try:
        country: Optional[Dict] = row["Title_y"]
    except:
        country = None
    postcode: Optional[str] = (
        row["AddressInfo.Postcode"]
    )
    postcode = (
        "".join([s for s in postcode if s.isdigit()])
        if postcode is not None
        else ""
    )
    town: Optional[str] = (
        row["AddressInfo.Town"]
    )
    if town is None:
        town = ""
    state_old: Optional[str] = (
        row["AddressInfo.StateOrProvince"]
    )
    if state_old is None:
        state_old = ""
    country: Optional[str] = row["ISOCode"]
    street: Optional[str] = (
        row["AddressInfo.AddressLine1"]
    )
    if len(postcode) != 5:
        log.warning(
            f"Postcode {postcode} of town {town} is not of length 5! Will set postcode to None!"
        )
        postcode = None
    if (len(town) < 2) | (not all(not s.isdigit() for s in town)):
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
    map_address.station_id=station_id
    map_address.street=street
    map_address.town=town
    map_address.postcode=postcode
    map_address.district_old=None
    map_address.state_old=state_old
    map_address.country=country
    return map_address