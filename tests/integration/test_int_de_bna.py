"""Integration tests for the crawler of the BNA pipeline."""

import os
import tempfile

import pandas as pd
import pytest

from charging_stations_pipelines.pipelines.de.bna_crawler import get_bna_data
from charging_stations_pipelines.shared import load_excel_file
from tests.test_utils import verify_schema_follows

EXPECTED_DATA_SCHEMA = {
    "Betreiber": "object",
    "Straße": "object",
    "Hausnummer": "object",
    "Adresszusatz": "object",
    "Postleitzahl": "object",
    "Ort": "object",
    "Bundesland": "object",
    "Kreis/kreisfreie Stadt": "object",
    "Breitengrad": "object",
    "Längengrad": "object",
    "Inbetriebnahmedatum": "object",
    "Nennleistung Ladeeinrichtung [kW]": "object",
    "Art der Ladeeinrichung": "object",
    "Anzahl Ladepunkte": "object",
    "Steckertypen1": "object",
    "P1 [kW]": "object",
    "Public Key1": "object",
    "Steckertypen2": "object",
    "P2 [kW]": "object",
    "Public Key2": "object",
    "Steckertypen3": "object",
    "P3 [kW]": "object",
    "Public Key3": "object",
    "Steckertypen4": "object",
    "P4 [kW]": "object",
    "Public Key4": "object",
}


@pytest.fixture(scope="module")
def bna_data():
    """Setup method for tests. Executes once at the beginning of the test session (and not before each test)."""
    # Download to a temporary file
    with tempfile.NamedTemporaryFile() as temp_file:
        bna_file_name = temp_file.name

        # Download real BNA data Current direct link:
        # https://data.bundesnetzagentur.de/Bundesnetzagentur/SharedDocs/Downloads/DE/Sachgebiete/Energie
        # /Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister.xlsx
        get_bna_data(bna_file_name)

        bna_in_data: pd.DataFrame = load_excel_file(bna_file_name)

        yield bna_file_name, bna_in_data


@pytest.mark.integration_test
def test_file_size(bna_data):
    bna_file_name, _ = bna_data
    # Check file size of the downloaded file
    assert os.path.getsize(bna_file_name) >= 8_602_458  # ~ 9 MB


@pytest.mark.integration_test
def test_dataframe_schema(bna_data):
    _, bna_in_data = bna_data
    # Check schema of the downloaded Excel file
    assert verify_schema_follows(bna_in_data, EXPECTED_DATA_SCHEMA), "Mismatch in schema of the downloaded Excel file!"


@pytest.mark.integration_test
def test_dataframe_shape(bna_data):
    _, bna_in_data = bna_data
    # Check shape of the dataframe
    # Not exact check, because file grows over time
    # Expected: at least 54,223 rows and 23 columns
    num_rows, num_cols = bna_in_data.shape
    assert num_rows >= 54_223, "Mismatch in dataframe shape: too few rows!"
    assert num_cols >= 23, "Mismatch in dataframe shape: too few columns!"
