"""This module contains the OpenChargeMap (OCM) extractor pipeline."""

import json
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd

from charging_stations_pipelines.file_utils import create_success_marker_file
from charging_stations_pipelines.shared import JSON

logger = logging.getLogger(__name__)


def standardize_country_codes(country_code: str) -> str:
    """Standardizes country codes to ISO 3166-1 alpha-2 country codes."""
    country_codes_map = {"NOR": "NO", "SWE": "SE"}
    return country_codes_map.get(country_code, country_code)


def load_and_normalize_pois(in_dir: Path, refs_data_in_file: Path) -> tuple[list[dict[str, Any]], pd.DataFrame, JSON]:
    """Loads and normalizes the OpenChargeMap (OCM) POIs data."""
    # Load non-normalized POIs data
    pois_non_normalized: list[dict[str, Any]] = []
    for subdir, dirs, files in os.walk(str(in_dir)):
        for file in files:
            with open(os.path.join(subdir, file)) as file:
                pois_non_normalized.append(json.load(file))

    # Normalize POIs data
    pois_normalized: pd.DataFrame = pd.json_normalize(pois_non_normalized)

    # Load reference data
    with refs_data_in_file.open() as file:
        refs_data: JSON = json.load(file)

    return pois_non_normalized, pois_normalized, refs_data


def merge_country_pois(pois_non_normalized: list[dict[str, Any]],  pois_normalized: pd.DataFrame, refs_data: JSON,
                       out_file: Path) -> None:
    """Merges the OpenChargeMap (OCM) POIs data with the reference data."""
    connection_types: pd.DataFrame = pd.json_normalize(refs_data["ConnectionTypes"])

    connection_frame = pd.json_normalize(pois_non_normalized, record_path=["Connections"], meta=["UUID"])
    connection_frame = pd.merge(
            left=connection_frame,
            right=connection_types,
            how="left",
            left_on="ConnectionTypeID",
            right_on="ID",
            validate="many_to_one"
    )
    connection_frame_grouped = connection_frame.groupby("UUID").agg(list)
    connection_frame_grouped.reset_index(inplace=True)
    connection_frame_grouped["ConnectionsEnriched"] = connection_frame_grouped.apply(
        lambda x: x.to_frame(), axis=1
    )

    data = pd.merge(
            pois_normalized,
            connection_frame_grouped[["ConnectionsEnriched", "UUID"]],
            how="left",
            on="UUID",
            validate="one_to_many",
    )

    address_info: pd.DataFrame = pd.json_normalize(refs_data["Countries"])
    address_info = address_info.rename(columns={"ID": "CountryID"})
    pd_merged_with_countries = pd.merge(
            data,
            address_info,
            how="left",
            left_on="AddressInfo.CountryID",
            right_on="CountryID",
            validate='many_to_one'
    )

    operators: pd.DataFrame = pd.json_normalize(refs_data["Operators"])
    operators = operators.rename(columns={"ID": "OperatorIDREF"})
    pd_merged_with_operators = pd.merge(
            pd_merged_with_countries,
            operators,
            how="left",
            left_on="OperatorID",
            right_on="OperatorIDREF",
            validate="many_to_one"
    )

    out_file.parent.mkdir(parents=True, exist_ok=True)
    pd_merged_with_operators.reset_index(drop=True).to_json(out_file, orient="index")


def merge_ocm_pois(in_dir: Path, out_file: Path, country_code: str) -> None:
    """Merges Open Charge Map (OCM) raw data for a given country and saves it to a specified file."""
    country_code = standardize_country_codes(country_code)
    per_country_in_dir = in_dir / f"{country_code}"

    # Some countries are not covered by the OCM data source (e.g. Vatican City (VA))
    if per_country_in_dir.exists():
        # Merge country POI files into one normalized country file
        pois_non_normalized, pois_normalized, refs_data = load_and_normalize_pois(
                per_country_in_dir, in_dir / "referencedata.json")
        merge_country_pois(pois_non_normalized, pois_normalized, refs_data, out_file)

        # After downloads have finished, place success marker file inside the output data folder
        create_success_marker_file(out_file.parent)
    else:
        # Or, create an empty file
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.touch()
