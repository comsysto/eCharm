"""Mapper for the BNA data source."""
import hashlib
import logging
import math
from numbers import Number
from typing import Optional

import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

import charging_stations_pipelines.pipelines.de as bna
from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.shared import check_coordinates, str_strip_whitespace

logger = logging.getLogger(__name__)


def map_station_bna(row: pd.Series):
    """Maps the data from the given pandas Series (row) to create a Station object for storage in the DB."""
    lat = check_coordinates(row["Breitengrad"])
    long = check_coordinates(row["Längengrad"])

    new_station = Station()

    new_station.country_code = "DE"

    new_station.data_source = bna.DATA_SOURCE_KEY
    new_station.source_id = hashlib.sha256(
        f"{lat}{long}{new_station.data_source}".encode()
    ).hexdigest()

    new_station.operator = row["Betreiber"]
    new_station.point = from_shape(Point(float(long), float(lat)))
    new_station.date_created = row["Inbetriebnahmedatum"].strftime("%Y-%m-%d")

    return new_station


def map_address_bna(row: pd.Series, station_id) -> Address:
    """Maps the data from the given pandas Series (row) to create an Address object for storage in the DB."""
    postcode = str_strip_whitespace(row.get("Postleitzahl"))
    town = str_strip_whitespace(row.get("Ort"))
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

    address = Address()

    address.station_id = station_id
    address.street = (
        str_strip_whitespace(row.get("Straße"))
        + " "
        + str_strip_whitespace(row.get("Hausnummer"))
    )
    address.town = town
    address.postcode = postcode
    address.district = row["Kreis/kreisfreie Stadt"]
    address.state = row["Bundesland"]
    address.country = "DE"

    return address


def map_charging_bna(row: pd.Series, station_id):
    """Maps the data from the given pandas Series (row) to create a Charging object for storage in the DB."""
    total_kw: Optional[float] = row["Nennleistung Ladeeinrichtung [kW]"]
    station_raw = dict(row)

    if isinstance(total_kw, str):
        try:
            total_kw = float(total_kw.replace(",", "."))
            logger.debug(f"Converting total_kw from string {total_kw} to int!")
        except Exception as conversionErr:
            logger.warning(
                f"Failed to convert string {total_kw} to Number! Will set total_kw to None! {conversionErr}"
            )
            total_kw = None

    if isinstance(total_kw, Number) and math.isnan(total_kw):
        total_kw = None

    if not isinstance(total_kw, Number):
        logger.warning(
            f"Cannot process total_kw {total_kw} with type {type(total_kw)}! Will set total_kw to None!"
        )
        total_kw = None

    # kw_list
    kw_list: list[float] = []
    for k, v in station_raw.items():
        if not (("P" in k) & ("[kW]" in k)):
            continue
        if pd.isna(v):
            continue
        if isinstance(v, str):
            if "," in v:
                v: str = v.replace(",", ".")
                logger.debug(
                    "Replaced coma with point for string to float conversion of kw!"
                )
            try:
                float_kw: float = float(v)
                kw_list += [float_kw]
            except Exception:
                logger.warning(
                    f"Failed to convert kw string {v} to float! Will not add this kw entry to list!"
                )
        if isinstance(v, Number):
            kw_list += [v]

    capacity: Optional[int] = row["Anzahl Ladepunkte"]
    if len(kw_list) != row["Anzahl Ladepunkte"]:
        logger.warning(f"kw_list {kw_list} length does not equal capacity {capacity}!")

    # Stations with only Schuko-Steckern are no charging stations for cars.
    # if kw_list and max(kw_list) < MIN_KW:
    #    raise ValueError("Max electrical power smaller than %d kW" % MIN_KW)
    # ampere_list not available
    # volt_list not available
    # socket_type_list
    socket_types_infos: list[str] = [
        v
        for k, v in station_raw.items()
        if ("Steckertypen" in k) & (isinstance(v, str)) & (not pd.isnull(v))
    ]
    socket_type_list: list[str] = []
    dc_support: bool = False
    for socket_types_info in socket_types_infos:
        tmp_socket_info: list[str] = socket_types_info.split(",")
        if (not dc_support) & (
            any(["DC" in s for s in tmp_socket_info])
        ):  # TODO: find more reliable way!
            dc_support = True
        socket_type_list += socket_types_info.split(",")
    kw_list_len: int = len(kw_list)
    if len(kw_list) != capacity:
        logger.warning(
            f"Difference between length of kw_list {kw_list_len} and capacity {capacity}!"
        )

    charging = Charging()
    charging.station_id = station_id
    charging.capacity = capacity
    charging.kw_list = kw_list
    charging.ampere_list = None
    charging.volt_list = None
    charging.socket_type_list = socket_type_list
    charging.dc_support = dc_support
    charging.total_kw = total_kw
    charging.max_kw = max(kw_list) if kw_list else None

    return charging
