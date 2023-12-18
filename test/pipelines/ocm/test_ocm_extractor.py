"""Test the OCM extractor."""
from pathlib import Path

from charging_stations_pipelines import PROJ_DATA_DIR


# FIXME
def test_fetcher():
    pass


# FIXME
def test_data_path():
    assert Path(__file__).parents[3] / 'data' == PROJ_DATA_DIR
