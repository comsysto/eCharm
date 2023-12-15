"""Utility functions for file handling."""

import json
import os
from pathlib import Path

import pandas as pd
import requests

from charging_stations_pipelines import JSON


def create_success_marker_file(out_dir: Path) -> None:
    """Creates a success marker file in the given directory."""
    out_dir.mkdir(parents=True, exist_ok=True)
    success_marker_file = out_dir / ".SUCCESS"
    success_marker_file.touch()


def is_data_present(path: Path) -> bool:
    """Checks if the data is present in the given directory."""
    return (path / '.SUCCESS').exists()


def append_country_code_to_file_name(filename: str, country_code: str) -> str:
    """Constructs a filename with the given country code."""
    base_name, *extensions = filename.split('.')
    new_filename = f"{base_name}_{country_code.lower()}"
    return '.'.join([new_filename] + extensions)


def load_json_file(file_path: os.PathLike) -> JSON:
    """Loads a json file into a dictionary."""
    with open(file_path) as file:
        data = json.load(file)
    return data


def load_excel_file(path: os.PathLike) -> pd.DataFrame:
    """Loads an excel file into a pandas dataframe."""
    # noinspection PyArgumentList
    df = pd.read_excel(path, engine="openpyxl")
    # Set the column names to the values in the 10th row
    df.columns = df.iloc[9]
    # Drop the comments in the Excel
    df_dropped = df[10:]
    return df_dropped


def download_file(url: str, target_file: Path) -> None:
    """Downloads a file from the given url and saves it to the given target file."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers).content
    target_file.parent.mkdir(parents=True, exist_ok=True)
    with target_file.open("wb") as file:
        file.write(response)
