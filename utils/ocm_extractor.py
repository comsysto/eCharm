import json
import os
import pathlib
import shutil

import git
import pandas as pd

from utils.logging_utils import log


def ocm_extractor(tmp_file_path: str):
    # Dataframes
    f_stations = pd.DataFrame()
    f_connection = pd.DataFrame()
    f_countries = pd.DataFrame()
    f_operators = pd.DataFrame()

    # function to delete the repo
    def delete_directory(direct):
        # getting the folder path from the user
        folder_path = direct

        try:
            shutil.rmtree(folder_path)
        except OSError as e:
            log.warn(f"Delete directory: {e.filename} - {e.strerror}.")

    directory_to_delete = os.path.join(
        pathlib.Path(tmp_file_path).parent.resolve(), "ocm-export"
    )
    # Delete the repo before cloning
    delete_directory(directory_to_delete)

    # get data from ocm Repo
    try:
        git.Repo.clone_from(
            "https://github.com/openchargemap/ocm-export", directory_to_delete
        )
    except:
        print("no need to clone")
    for subdir, dirs, files in os.walk(os.path.join(directory_to_delete, "data", "DE")):
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

    with open(
        os.path.join(directory_to_delete, "data", "referencedata.json", "r+")
    ) as f:
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

    pd_merged_with_operators_connections_titles = (
        pd_merged_with_operators_connections_titles.rename(
            columns={"ID_x": "id_con_titles"}
        )
    )
    pd_merged_with_operators_connections_titles = (
        pd_merged_with_operators_connections_titles.rename(
            columns={"Title_x": "title_connection"}
        )
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
