import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
from typing import Dict, List

import pandas as pd
from packaging import version

logger = logging.getLogger(__name__)


def reference_data_to_frame(data: List[Dict]) -> pd.DataFrame:
    frame: pd.DataFrame = pd.DataFrame(data)
    frame.set_index("ID", inplace=True)
    frame.sort_index(inplace=True)
    return frame


def merge_connection_types(
        connection: pd.DataFrame, reference_data: pd.DataFrame
) -> pd.DataFrame:
    connection_ids: pd.Series = (
        connection["ConnectionTypeID"].dropna().drop_duplicates()
    )
    return connection.merge(
            reference_data.loc[connection_ids],
            how="left",
            left_on="ConnectionTypeID",
            right_index=True,
    )


def merge_address_infos(
        address_info: pd.Series, reference_data: pd.DataFrame
) -> pd.DataFrame:
    return pd.concat([address_info, reference_data.loc[address_info["CountryID"]]])


def merge_with_reference_data(
        row: pd.Series,
        connection_types: pd.DataFrame,
        address_info: pd.DataFrame,
        operators: pd.DataFrame,
):
    row["Connections"] = merge_connection_types(
            connection=pd.json_normalize(row["Connections"]),
            reference_data=connection_types,
    )
    row["AddressInfo"] = merge_address_infos(
            address_info=pd.Series(row["AddressInfo"]), reference_data=address_info
    )
    row["OperatorID"] = operators.loc[row["OperatorID"]]
    return row


def merge_connections(row, connection_types):
    frame = pd.DataFrame(row)
    if "ConnectionTypeID" not in frame.columns:
        return frame
    return pd.merge(frame, connection_types, how="left", left_on="ConnectionTypeID", right_on="ID")


def ocm_extractor(tmp_file_path: str, country_code: str):
    """This method extracts Open Charge Map (OCM) data for a given country and saves it to a specified file."""
    # OCM export contains norwegian data under country code "NO" and that's why we need to rename it to "NO"
    if country_code == "NOR":
        country_code = "NO"
    if country_code == "SWE":
        country_code = "SE"

    project_data_dir: str = pathlib.Path(tmp_file_path).parent.resolve().name
    data_root_dir: str = os.path.join(project_data_dir, "ocm-export")
    data_dir: str = os.path.join(data_root_dir, f"data/{country_code}")

    try:
        git_version_raw: str = subprocess.check_output(["git", "--version"]).decode()
        pattern = re.compile(r"\d+\.\d+\.\d+")
        match = re.search(pattern, git_version_raw).group()
        git_version: version = version.parse(match)
    except FileNotFoundError as e:
        raise RuntimeError(f"Git is not installed! {e}")
    except TypeError as e:
        raise RuntimeError(f"Could not parse git version! {e}")
    else:
        if git_version < version.parse("2.25.0"):
            logger.warning(f"found git version {git_version}, extracted from git"
                           f" --version: {git_version_raw} and regex match {match}")
            raise RuntimeError("Git version must be >= 2.25.0!")

    if (not os.path.isdir(data_dir)) or len(os.listdir(data_dir)) == 0:
        shutil.rmtree(data_root_dir, ignore_errors=True)
        subprocess.call(
                [
                    "git",
                    "clone",
                    "https://github.com/openchargemap/ocm-export",
                    "--no-checkout",
                    "--depth",
                    "1",
                ],
                cwd=project_data_dir,
                stdout=subprocess.PIPE,
        )
        subprocess.call(
                ["git", "sparse-checkout", "init", "--cone"],
                cwd=data_root_dir,
                stdout=subprocess.PIPE,
        )
        subprocess.call(
                ["git", "sparse-checkout", "set", f"data/{country_code}"],
                cwd=data_root_dir,
                stdout=subprocess.PIPE,
        )
        subprocess.call(["git", "checkout"], cwd=data_root_dir, stdout=subprocess.PIPE)
    else:
        subprocess.call(["git", "pull"], cwd=data_root_dir, stdout=subprocess.PIPE)

    records: List = []
    for subdir, dirs, files in os.walk(os.path.join(data_dir)):
        for file in files:
            with open(os.path.join(subdir, file)) as f:
                records += [(json.load(f))]
    data: pd.DataFrame = pd.json_normalize(records)

    with open(os.path.join(data_dir, "..", "referencedata.json"), "r+") as f:
        data_ref: Dict = json.load(f)

    connection_types: pd.DataFrame = pd.json_normalize(data_ref["ConnectionTypes"])
    connection_frame = pd.json_normalize(
            records, record_path=["Connections"], meta=["UUID"]
    )
    connection_frame = pd.merge(
            connection_frame,
            connection_types,
            how="left",
            left_on="ConnectionTypeID",
            right_on="ID",
    )
    connection_frame_grouped = connection_frame.groupby("UUID").agg(list)
    connection_frame_grouped.reset_index(inplace=True)
    connection_frame_grouped["ConnectionsEnriched"] = connection_frame_grouped.apply(
            lambda x: x.to_frame(), axis=1
    )
    data = pd.merge(
            data,
            connection_frame_grouped[["ConnectionsEnriched", "UUID"]],
            how="left",
            on="UUID",
    )

    address_info: pd.DataFrame = pd.json_normalize(data_ref["Countries"])
    address_info = address_info.rename(columns={"ID": "CountryID"})
    pd_merged_with_countries = pd.merge(
            data,
            address_info,
            left_on="AddressInfo.CountryID",
            right_on="CountryID",
            how="left",
    )

    operators: pd.DataFrame = pd.json_normalize(data_ref["Operators"])
    operators = operators.rename(columns={"ID": "OperatorIDREF"})
    pd_merged_with_operators = pd.merge(
            pd_merged_with_countries,
            operators,
            left_on="OperatorID",
            right_on="OperatorIDREF",
            how="left",
    )

    pd_merged_with_operators.reset_index(drop=True).to_json(
            tmp_file_path, orient="index"
    )
