import math
import pandas as pd

from models.charging import Charging
from typing import List, Optional
from numbers import Number
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
            #log.warn("Found nan in total_kw! Will set total_kw to None!")
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
        log.warning(
            f"kw_list {kw_list} length does not equal capacity {capacity}!"
        )

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
    charging.ampere_list = None
    charging.volt_list = None
    charging.socket_type_list = socket_type_list
    charging.dc_support = dc_support
    charging.total_kw = total_kw
    charging.max_kw = max(kw_list) if kw_list else None
    charging = _clean_attributes(charging)
    return charging
