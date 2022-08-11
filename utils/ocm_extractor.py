import json
import os
import pathlib
import shutil
import subprocess

import pandas as pd
from packaging import version


def ocm_extractor(tmp_file_path: str):
    # Dataframes
    f_stations = pd.DataFrame()
    f_connection = pd.DataFrame()
    f_countries = pd.DataFrame()
    f_operators = pd.DataFrame()

    data_dir: str = pathlib.Path(tmp_file_path).parent.resolve()
    ocm_data_root_dir: str = os.path.join(data_dir, "ocm-export")
    ocm_data_dir: str = os.path.join(ocm_data_root_dir, "data/De")

    try:
        git_version_raw: str = subprocess.check_output(["git", "--version"])
        git_version: version = version.parse(
            git_version_raw.decode("utf-8").strip().split(" ")[-1]
        )
    except FileNotFoundError as e:
        raise RuntimeError(f"Git is not installed! {e}")
    except TypeError as e:
        raise RuntimeError(f"Could not parse git version! {e}")
    else:
        if git_version < version.parse("2.25.0"):
            raise RuntimeError("Git version must be >= 2.25.0!")

    if (not os.path.isdir(ocm_data_dir)) or len(os.listdir(ocm_data_dir)) == 0:
        shutil.rmtree(ocm_data_root_dir, ignore_errors=True)
        subprocess.call(
            [
                "git",
                "clone",
                "https://github.com/openchargemap/ocm-export",
                "--no-checkout",
                "--depth",
                "1",
            ],
            cwd=data_dir,
            stdout=subprocess.PIPE,
        )
        subprocess.call(
            ["git", "sparse-checkout", "init", "--cone"],
            cwd=ocm_data_root_dir,
            stdout=subprocess.PIPE,
        )
        subprocess.call(
            ["git", "sparse-checkout", "set", "data/DE"],
            cwd=ocm_data_root_dir,
            stdout=subprocess.PIPE,
        )
        subprocess.call(
            ["git", "checkout"], cwd=ocm_data_root_dir, stdout=subprocess.PIPE
        )
    else:
        subprocess.call(["git", "pull"], cwd=ocm_data_root_dir, stdout=subprocess.PIPE)

    for subdir, dirs, files in os.walk(os.path.join(data_dir, "data", "DE")):
        for file in files:
            # Opening JSON file
            f = open(os.path.join(subdir, file))
            # returns JSON object as a dictionary
            data = json.load(f)
            # put all the jsons in one list and join it flatten the connections
            data_normalized_2 = pd.json_normalize(
                data, record_path=["Connections"], meta=["UUID"]
            )
            data_normalized_2 = data_normalized_2.rename(
                columns={"ID": "ConncectionID"}
            )
            data_normalized = pd.json_normalize(data, max_level=1)
            pd_merged_with_connections = pd.merge(
                data_normalized,
                data_normalized_2,
                left_on="UUID",
                right_on="UUID",
                how="left",
            )
            f_stations = f_stations.append(
                pd_merged_with_connections, ignore_index=True
            )

    with open(os.path.join(ocm_data_dir, "..", "referencedata.json"), "r+") as f:
        data_ref = json.load(f)

    for _connection_type in data_ref["ConnectionTypes"]:
        connection_normalized = pd.json_normalize(_connection_type)
        f_connection = f_connection.append(connection_normalized, ignore_index=True)

    pd_merged_with_connections_types = pd.merge(
        f_stations, f_connection, left_on="ConnectionTypeID", right_on="ID", how="left"
    )

    for _country_row in data_ref["Countries"]:
        country_normalized = pd.json_normalize(_country_row)
        country_normalized = country_normalized.rename(columns={"ID": "CountryID"})
        f_countries = f_countries.append(country_normalized, ignore_index=True)

    pd_merged_with_countries = pd.merge(
        pd_merged_with_connections_types,
        f_countries,
        left_on="AddressInfo.CountryID",
        right_on="CountryID",
        how="left",
    )

    for _operator_row in data_ref["Operators"]:
        operator_normalized = pd.json_normalize(_operator_row)
        operator_normalized = operator_normalized.rename(
            columns={"ID": "OperatorIDREF"}
        )
        f_operators = f_operators.append(operator_normalized, ignore_index=True)

    pd_merged_with_operators = pd.merge(
        pd_merged_with_countries,
        f_operators,
        left_on="OperatorID",
        right_on="OperatorIDREF",
        how="left",
    )

    pd_merged_with_operators_connections_titles = (
        pd_merged_with_operators.groupby(["ID_x"])["Title_x"]
        .apply(lambda x: ", ".join(x.astype(str)))
        .reset_index()
    )

    pd_merged_with_operators_connections_titles = pd_merged_with_operators_connections_titles.rename(
        columns={"ID_x": "id_con_titles"}
    )
    pd_merged_with_operators_connections_titles = pd_merged_with_operators_connections_titles.rename(
        columns={"Title_x": "title_connection"}
    )

    pd_merged_with_operators = pd_merged_with_operators.drop_duplicates(
        subset="ID_x", keep="last"
    ).reset_index()

    pd_merged_with_operators_merged = pd.merge(
        pd_merged_with_operators,
        pd_merged_with_operators_connections_titles,
        left_on="ID_x",
        right_on="id_con_titles",
        how="left",
    )

    pd_merged_with_operators_merged.to_json(tmp_file_path)
