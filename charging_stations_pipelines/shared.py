import configparser
import os
import pathlib

import pandas as pd
import requests


def reject_if(test: bool, error_message: str = ""):
    if test:
        raise RuntimeError(error_message)


def init_config():
    config: configparser = configparser.RawConfigParser()
    config.read(os.path.join(os.path.join(current_dir, "config", "config.ini")))
    return config


def load_excel_file(path):
    # Read excel file as pandas dataframe
    df = pd.read_excel(path, engine="openpyxl")
    df.columns = df.iloc[9]
    # Drop the comments in the excel
    df_dropped = df[10:]
    return df_dropped


def string_to_bool(bool_string: str) -> bool:
    return bool_string.lower() in ['True', 'true', '1', 't']


def download_file(url, target_file):
    resp = requests.get(url)
    output = open(target_file, "wb")
    output.write(resp.content)
    output.close()


current_dir = os.path.join(pathlib.Path(__file__).parent.parent.resolve())
config: configparser = init_config()
