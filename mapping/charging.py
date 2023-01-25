import math
from numbers import Number
from typing import List, Optional

import pandas as pd

from models.charging import Charging
from utils.logging_utils import log

# max sockets/charging points per charging station
MAX_CAPACITY = 4

# average capacity across all charging stations is set when capacity>MAX_CAPACITY
AVG_CAPACITY = 2

# min electrical power (in kW) per charging station
MIN_KW = 5


def _clean_attributes(charging: Charging):
    if charging.capacity and charging.capacity > MAX_CAPACITY:
        charging.charging = AVG_CAPACITY
    return charging


def map_charging_bna(row, station_id):
    total_kw: Optional[float] = row["Anschlussleistung"]
    station_raw = dict(row)
    if isinstance(total_kw, str):
        try:
            total_kw = float(total_kw.replace(",", "."))
            log.debug(f"Converting total_kw from string {total_kw} to int!")
        except Exception as conversionErr:
            log.warning(
                f"Failed to convert string {total_kw} to Number! Will set total_kw to None! {conversionErr}"
            )
            total_kw = None
    if isinstance(total_kw, Number):
        if math.isnan(total_kw):
            # log.warn("Found nan in total_kw! Will set total_kw to None!")
            total_kw = None
    if not isinstance(total_kw, Number):
        log.warn(
            f"Cannot process total_kw {total_kw} with type {type(total_kw)}! Will set total_kw to None!"
        )
        total_kw = None

    # kw_list
    kw_list: List[float] = []
    for k, v in station_raw.items():
        if not (("P" in k) & ("[kW]" in k)):
            continue
        if pd.isnull(v) | pd.isna(v):
            continue
        if isinstance(v, str):
            if "," in v:
                v: str = v.replace(",", ".")
                log.debug(
                    "Replaced coma with point for string to float conversion of kw!"
                )
            try:
                float_kw: float = float(v)
                kw_list += [float_kw]
            except:
                log.warn(
                    f"Failed to convert kw string {v} to float! Will not add this kw entry to list!"
                )
        if isinstance(v, Number):
            kw_list += [v]

    capacity: Optional[int] = row["Anzahl Ladepunkte"]
    if len(kw_list) != row["Anzahl Ladepunkte"]:
        log.warning(f"kw_list {kw_list} length does not equal capacity {capacity}!")

    # Stations with only Schuko-Steckern are no charging stations for cars.
    # if kw_list and max(kw_list) < MIN_KW:
    #    raise ValueError("Max electrical power smaller than %d kW" % MIN_KW)
    # ampere_list not available
    # volt_list not available
    # socket_type_list
    socket_types_infos: List[str] = [
        v
        for k, v in station_raw.items()
        if ("Steckertypen" in k) & (isinstance(v, str)) & (not pd.isnull(v))
    ]
    socket_type_list: List[str] = []
    dc_support: bool = False
    for socket_types_info in socket_types_infos:
        tmp_socket_info: List[str] = socket_types_info.split(",")
        if (not dc_support) & (
            any(["DC" in s for s in tmp_socket_info])
        ):  # TODO: find more reliable way!
            dc_support = True
        socket_type_list += socket_types_info.split(",")
    kw_list_len: int = len(kw_list)
    if len(kw_list) != capacity:
        log.warning(
            f"Difference between length of kw_list {kw_list_len} and capacity {capacity}!"
        )

    charging = Charging()
    charging.station_id = station_id
    charging.capacity = capacity
    charging.kw_list = kw_list
    charging.ampere_list = []
    charging.volt_list = []
    charging.socket_type_list = socket_type_list
    charging.dc_support = dc_support
    charging.total_kw = total_kw
    charging.max_kw = max(kw_list) if kw_list else None
    charging = _clean_attributes(charging)
    return charging


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


def map_charging_osm(entry, station_id):
    charging = Charging()
    charging.station_id = station_id
    # TODO: read charging info
    return charging


def map_charging_fra(row, station_id):

    mapped_charging_fra:Charging = Charging()
    mapped_charging_fra.capacity = row["nbre_pdc"]
    
    return mapped_charging_fra


def map_charging_gb(entry, station_id):
    mapped_charging_gb:Charging = Charging()
    mapped_charging_gb.capacity = entry.get("RatedOutputCurrent")
    #above is not correct information (just there for testing purposes)
    # TODO:find way to count the points of charge for each station
    #since this information is not available in the json

    return mapped_charging_gb
