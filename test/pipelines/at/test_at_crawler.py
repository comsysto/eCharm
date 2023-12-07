"""Test the AT crawler pipeline with mock tests."""

import json
import logging
from unittest import mock

import pytest

import charging_stations_pipelines.pipelines.at.econtrol_crawler as ec
from charging_stations_pipelines.pipelines.at.econtrol_crawler import __name__ as test_module_name


def test_get_paginated_stations():
    test_url = "https://test.server.com/stations" # NOSONAR
    test_headers = {'Authorization': 'test_auth', 'User-Agent': 'Mozilla/5.0'} # NOSONAR
    mock_response_content = {
        'totalResults': 2,
        'fromIndex':    0,
        'endIndex':     1,
        'stations':     [
            {'station_id': 1, 'name': 'Test Station 1'},
            {'station_id': 2, 'name': 'Test Station 2'},
        ]
    }

    with mock.patch('requests.Session') as mock_session:
        mock_session.return_value.get.return_value.json.return_value = mock_response_content
        result = list(ec._get_paginated_stations(test_url, test_headers))

    assert len(result) == 1
    assert result[0] == mock_response_content


def test_get_data(caplog: object):
    test_data_path = "/tmp/test_data.ndjson"

    mock_response_content = {
        'totalResults': 2,
        'fromIndex':    0,
        'endIndex':     1,
        'stations':     [
            {'station_id': 1, 'name': 'Test Station 1'},
            {'station_id': 2, 'name': 'Test Station 2'},
        ]
    }

    # Test logging (1/2): test file size logged
    caplog.set_level(logging.DEBUG, logger=test_module_name)

    with (mock.patch('requests.Session') as mock_session,
          mock.patch('builtins.open', mock.mock_open()) as mock_file,
          mock.patch('os.path.getsize', return_value=1077) as mock_file_size):
        mock_session.return_value.get.return_value.json.return_value = mock_response_content
        ec.get_data(test_data_path)

    # Test written data (JSON array of Stations objects)
    # noinspection PyArgumentList
    written_data_text = ''.join([call.args[0] for call in mock_file().write.call_args_list])
    written_json_objects = [json.loads(line) for line in written_data_text.splitlines()]
    assert mock_response_content['stations'] == written_json_objects


def test_get_data_empty_response(caplog):
    """Test the case when the response does not contain any stations."""
    test_data_path = "/tmp/test_data.ndjson"

    mock_response_content = {
        'totalResults': 0,
        'fromIndex':    0,
        'endIndex':     0,
        'stations':     []
    }

    # Test logging (1/2): test file size logged
    caplog.set_level(logging.DEBUG, logger=test_module_name)

    with (mock.patch('requests.Session') as mock_session,
          mock.patch('builtins.open', mock.mock_open()) as mock_file,
          mock.patch('os.path.getsize', return_value=0) as mock_file_size):
        mock_session.return_value.get.return_value.json.return_value = mock_response_content
        ec.get_data(test_data_path)

    mock_file.assert_called_once_with(test_data_path, 'w')
    # noinspection PyArgumentList
    assert mock_file().write.called is False


def test_get_paginated_stations_key_error():
    """Test the case when the response does not contain expected keys."""
    test_url = "https://test.server.com/stations"
    test_headers = {'Authorization': 'test_auth', 'User-Agent': 'Mozilla/5.0'}
    mock_response_content = {
        'totalResults': 9454,
        'fromIndex':    0,
        # Missing 'endIndex' key
    }

    with mock.patch('requests.Session') as mock_session:
        mock_session.return_value.get.return_value.json.return_value = mock_response_content
        # Expect a KeyError due to missing 'endIndex' key
        with pytest.raises(KeyError):
            list(ec._get_paginated_stations(test_url, test_headers))
