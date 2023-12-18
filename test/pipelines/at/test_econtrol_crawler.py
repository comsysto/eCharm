"""Test the AT pipeline crawler with mock tests."""

import io
import json
import logging
import os
import pathlib
from pathlib import Path
from typing import Final
from unittest import mock

import pytest

import charging_stations_pipelines
from charging_stations_pipelines.pipelines.at import econtrol_crawler
from charging_stations_pipelines.pipelines.at.econtrol_crawler import (
    __name__ as test_module_name,
)
# NOTE: "local_caplog" is a pytest fixture from test.shared.local_caplog
from test.shared import local_caplog, LogLocalCaptureFixture  # noqa: F401


@mock.patch("requests.Session.get")
def test_paginated_stations(mock_get):
    station_api_url = "https://api.test-charge.com/stations"
    station_pages = [
        {
            "totalResults": 13,
            "fromIndex": i * 3,
            "endIndex": min((i + 1) * 3 - 1, 13 - 1),
            "stations": [
                {f"id{j}": f"station{j}"} for j in range(i * 3 + 1, (i + 1) * 3)
            ],
        }
        for i in range(13 // 3 + 13 % 3)
    ]

    page_generator = econtrol_crawler.get_paginated_stations(
        station_api_url, headers={}
    )

    for idx, station_page in enumerate(station_pages):
        mock_get.return_value.json.return_value = station_page

        assert next(page_generator) == station_page
        (
            mock_get.assert_called_with(
                station_api_url,
                params={
                    "fromIndex": station_page["fromIndex"],
                    "endIndex": station_page["endIndex"],
                },
            )
        ) if idx else mock_get.assert_called_with(station_api_url)

    assert mock_get.call_count == len(station_pages)


def test_get_paginated_stations_key_error():
    """Test the case when the response does not contain expected keys."""
    test_url = "https://test.server.com/stations"
    test_headers = {"Authorization": "test_auth", "User-Agent": "Mozilla/5.0"}
    mock_response_content = {
        "totalResults": 9454,
        "fromIndex": 0,
        # Missing 'endIndex' key
    }

    with mock.patch("requests.Session") as mock_session:
        mock_session.return_value.get.return_value.json.return_value = (
            mock_response_content
        )

        # Expect a KeyError due to missing 'endIndex' key
        with pytest.raises(KeyError, match=r"endIndex"):
            list(econtrol_crawler.get_paginated_stations(test_url, test_headers))




@mock.patch.object(pathlib.Path, 'open', new_callable=mock.mock_open(), create=True)
@mock.patch.object(charging_stations_pipelines.pipelines.at.econtrol_crawler, 'get_paginated_stations')
@mock.patch.object(os, 'getenv')
@mock.patch.object(pathlib.Path, 'stat')
@mock.patch.object(pathlib.Path, 'mkdir')
def test_get_data_checks_api_calls_and_file_content(
        mock_path_mkdir,
        mock_path_stat,
        mock_getenv,
        mock_get_paginated_stations,
        mock_path_open,
        local_caplog: LogLocalCaptureFixture,
):
    # Prepare test data and mocks
    test_file_content_objs_iter = iter([{
            "totalResults": 100,
            "fromIndex": i * 10,
            "endIndex": min((i + 1) * 10 - 1, 100 - 1),
            "stations": [
                {f"id{j}": f"station{j}"}
                for j in range(i * 10 + 1, (i + 1) * 10 + 1)
            ],
        } for i in range(100 // 10 + 100 % 10)])

    test_data_path = Path("/tmp/test_data.ndjson")
    expected_test_file_size: Final[int] = 2184

    # Setting up mocks
    # Mocking Path.mkdir
    mock_path_mkdir.side_effect = lambda parents=False, exist_ok=False: FileExistsError if not parents else None
    # Mocking Path.stat(). Setting only st_size field to the expected file size
    mock_path_stat.return_value = os.stat_result((0, 0, 0, 0, 0, 0, expected_test_file_size, 0, 0, 0))

    # Mock open() to return an in-memory StringIO
    mock_string_io = mock.MagicMock()
    mock_string_io.__enter__.return_value = io.StringIO()
    mock_path_open.return_value = mock_string_io

    # Setup return value for get_paginated_stations
    mock_get_paginated_stations.return_value = test_file_content_objs_iter

    # Invoke method to be tested with mocked logging
    logger = logging.getLogger(test_module_name)
    with local_caplog(level=logging.DEBUG, logger=logger):
        # Call method under test... with mocked logging
        econtrol_crawler.get_data(test_data_path)

    # Assertions related to function calls and I/O
    mock_getenv.assert_called_with("ECONTROL_AT_AUTH")
    mock_get_paginated_stations.assert_called()

    # Get file contents from the mocked StringIO
    file_contents = mock_string_io.__enter__().getvalue()
    assert f"Downloaded file size: {expected_test_file_size} bytes" in local_caplog.logs
    assert len(file_contents) == expected_test_file_size

    # Check each JSON object of the in-memory file
    actual_file_content_objs = [json.loads(line) for line in file_contents.splitlines()]
    expected_objs = [{f"id{i}": f"station{i}"} for i in range(1, 100 + 1)]
    assert actual_file_content_objs == expected_objs


@mock.patch.object(pathlib.Path, 'open', new_callable=mock.mock_open(), create=True)
@mock.patch.object(charging_stations_pipelines.pipelines.at.econtrol_crawler, 'get_paginated_stations')
@mock.patch.object(os, 'getenv')
@mock.patch.object(pathlib.Path, 'stat')
@mock.patch.object(pathlib.Path, 'mkdir')
def test_get_data_checks_api_calls_and_file_content(
        mock_path_mkdir,
        mock_path_stat,
        mock_getenv,
        mock_get_paginated_stations,
        mock_path_open,
        local_caplog: LogLocalCaptureFixture,
):  # noqa: F811
    # Prepare test data and mocks
    test_file_content_objs_iter = iter([{
            "totalResults": 0,
            "fromIndex": 0,
            "endIndex": 0,
            "stations": [], }])
    test_data_path = Path("/tmp/test_data.ndjson")
    expected_test_file_size: Final[int] = 0

    # Setting up mocks
    # Mocking Path.mkdir
    mock_path_mkdir.side_effect = lambda parents=False, exist_ok=False: FileExistsError if not parents else None
    # Mocking Path.stat(). Setting only st_size field to the expected file size
    mock_path_stat.return_value = os.stat_result((0, 0, 0, 0, 0, 0, expected_test_file_size, 0, 0, 0))

    # Mock open() to return an in-memory StringIO
    mock_string_io = mock.MagicMock()
    mock_string_io.__enter__.return_value = io.StringIO()
    mock_path_open.return_value = mock_string_io

    # Setup return value for get_paginated_stations
    mock_get_paginated_stations.return_value = test_file_content_objs_iter

    # Invoke method to be tested with mocked logging
    logger = logging.getLogger(test_module_name)
    with local_caplog(level=logging.DEBUG, logger=logger):
        econtrol_crawler.get_data(test_data_path)

    # Assertions related to function calls and I/O
    mock_getenv.assert_called_with("ECONTROL_AT_AUTH")
    mock_get_paginated_stations.assert_called()

    # Check calls
    mock_get_paginated_stations.assert_called_once()
    assert mock_path_open.write.called is False

    # Get file contents from the mocked StringIO
    file_contents = mock_string_io.__enter__().getvalue()
    # Check logging and file size via log message
    assert f"Downloaded file size: {expected_test_file_size} bytes" in local_caplog.logs
    # Check file size
    assert len(file_contents) == expected_test_file_size
