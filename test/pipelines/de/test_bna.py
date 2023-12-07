import configparser
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

import charging_stations_pipelines.pipelines.de as de
from charging_stations_pipelines.pipelines.de.bna import BnaPipeline


@pytest.fixture
def pipeline():
    """Returns a BnaPipeline instance with a mocked session."""
    config = configparser.ConfigParser()
    config[de.DATA_SOURCE_KEY] = {"filename": "fake_file.xlsx"}
    session = MagicMock(spec=Session)

    pipeline = BnaPipeline(config=config, session=session)
    # pipeline.data_dir = '/fake_dir'

    return pipeline


# Note: this is just a placeholder test
# @patch("charging_stations_pipelines.shared.load_excel_file")
def test_pipeline_initialization(pipeline):
    assert pipeline is not None

    # TODO(cs-dieter-kling): implement mock tests
    # pipeline.retrieve_data()
    # pipeline.run()

# FIXME
# @patch('charging_stations_pipelines.shared.load_excel_file')
# def test_retrieve_data_offline(mock_load_excel_file, pipeline):
# Use MagicMock to create a mock file path
# mock_path = MagicMock(spec=pathlib.Path)
#
# with patch('pathlib.Path', return_value=mock_path):
#     # Run the method under test
#     pipeline.retrieve_data()
#
#     mock_path.mkdir.assert_called_once_with(parents=True, exist_ok=True)
#     mock_load_excel_file.assert_called_once_with('/fake_dir/fake_file.xlsx')
