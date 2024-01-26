"""Integration tests for the crawler of the France pipeline."""

import os
import tempfile
import pytest

from charging_stations_pipelines.pipelines.fr.france import FraPipeline
from test.shared import skip_if_github
from tests.test_utils import verify_schema_follows

EXPECTED_DATA_SCHEMA = {
    "id_station_itinerance": "object",
    "nom_operateur": "object",
    "consolidated_longitude": "float64",
    "consolidated_latitude": "float64",
    "date_mise_en_service": "object",
    "date_maj": "object",
    "nbre_pdc": "int64",
    "adresse_station": "object",
    "consolidated_commune": "object",
    "consolidated_code_postal": "float64",
}


@pytest.fixture(scope="module")
def fr_data():
    """Setup method for tests. Executes once at the beginning of the test session (and not before each test)."""
    # Download to a temporary file
    with tempfile.NamedTemporaryFile() as temp_file:
        FraPipeline.download_france_gov_file(temp_file.name)
        fr_dataframe = FraPipeline.load_csv_file(temp_file.name)
        yield temp_file.name, fr_dataframe


@pytest.mark.integration_test
@pytest.mark.skipif(skip_if_github(), reason="Skip the test when running on Github")
def test_download_france_gov_file(fr_data):
    """Test the download function."""
    fr_filename, _ = fr_data
    assert os.path.getsize(fr_filename) >= 1_000  # actual file is ~ 45 MB, just make sure it is not quasi empty here


@pytest.mark.integration_test
def test_dataframe_schema(fr_data):
    _, fr_dataframe = fr_data
    assert verify_schema_follows(fr_dataframe, EXPECTED_DATA_SCHEMA), "Mismatch in schema of the downloaded csv file!"
