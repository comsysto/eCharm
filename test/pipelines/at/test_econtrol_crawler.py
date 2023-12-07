"""Test the AT pipeline crawler with mock tests."""
import io
import json
import logging
from typing import Final
from unittest import mock

import pytest

from charging_stations_pipelines.pipelines.at import econtrol_crawler
from charging_stations_pipelines.pipelines.at.econtrol_crawler import (
    __name__ as test_module_name,
)
# "local_caplog" is pytest fixture from test.shared.local_caplog
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

    page_generator = econtrol_crawler._get_paginated_stations(
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
            list(econtrol_crawler._get_paginated_stations(test_url, test_headers))


@mock.patch("builtins.open", new_callable=mock.mock_open)
@mock.patch(
    "charging_stations_pipelines.pipelines.at.econtrol_crawler._get_paginated_stations"
)
@mock.patch("os.getenv")
@mock.patch("os.path.getsize")
def test_get_data(
    mock_getsize,
    mock_getenv,
    mock_get_paginated_stations,
    mock_open,
    local_caplog: LogLocalCaptureFixture,
):  # noqa: F811
    # Prepare test data and mocks
    expected_file_size: Final[int] = 2184
    tmp_data_path = "/tmp/test_data.ndjson"

    # Mock os.path.getsize() to return the expected file size
    mock_getsize.return_value = expected_file_size
    # Mock os.getenv() to return a dummy token
    mock_getenv.return_value = "test_token"
    # Mock open() to return a dummy file
    mock_file = mock.MagicMock()
    mock_file.__enter__.return_value = io.StringIO()  # In-memory file
    mock_open.return_value = mock_file
    # Mock _get_paginated_stations() to return 100 stations in 10 pages
    mock_get_paginated_stations.return_value = iter(
        [
            {
                "totalResults": 100,
                "fromIndex": i * 10,
                "endIndex": min((i + 1) * 10 - 1, 100 - 1),
                "stations": [
                    {f"id{j}": f"station{j}"}
                    for j in range(i * 10 + 1, (i + 1) * 10 + 1)
                ],
            }
            for i in range(100 // 10 + 100 % 10)
        ]
    )

    # Call method under test... with mocked logging
    logger = logging.getLogger(test_module_name)
    with local_caplog(level=logging.DEBUG, logger=logger):
        # Call method under test... with mocked logging
        econtrol_crawler.get_data(tmp_data_path)

    # Check calls
    mock_getenv.assert_called_with("ECONTROL_AT_AUTH")
    mock_get_paginated_stations.assert_called()

    # Get file contents
    file_contents = mock_file.__enter__().getvalue()

    # Check logging and file size via log message
    assert f"Downloaded file size: {expected_file_size} bytes" in local_caplog.logs

    # Check file size
    assert len(file_contents) == expected_file_size

    # Check objects from file, after reading it back from the in-memory file
    actual_file_content_objs = [json.loads(line) for line in file_contents.splitlines()]
    expected_objs = [{f"id{i}": f"station{i}"} for i in range(1, 100 + 1)]
    assert actual_file_content_objs == expected_objs


@mock.patch("builtins.open", new_callable=mock.mock_open)
@mock.patch(
    "charging_stations_pipelines.pipelines.at.econtrol_crawler._get_paginated_stations"
)
@mock.patch("os.getenv")
@mock.patch("os.path.getsize")
def test_get_data_empty_response(
    mock_getsize,
    mock_getenv,
    mock_get_paginated_stations,
    mock_open,
    local_caplog: LogLocalCaptureFixture,
):  # noqa: F811
    # Prepare test data and mocks
    expected_file_size: Final[int] = 0
    tmp_data_path = "/tmp/test_data.ndjson"
    mock_response_content = [
        {
            "totalResults": 0,
            "fromIndex": 0,
            "endIndex": 0,
            "stations": [],
        }
    ]

    # Mock os.path.getsize() to return the expected file size
    mock_getsize.return_value = expected_file_size
    # Mock os.getenv() to return a dummy token
    mock_getenv.return_value = "test_token"
    # Mock open() to return a dummy file
    mock_file = mock.MagicMock()
    mock_file.__enter__.return_value = io.StringIO()  # In-memory file
    mock_open.return_value = mock_file
    # Mock _get_paginated_stations() to return 100 stations in 10 pages
    mock_get_paginated_stations.return_value = iter(mock_response_content)

    # Call method under test... with mocked logging
    logger = logging.getLogger(test_module_name)
    with local_caplog(level=logging.DEBUG, logger=logger):
        # Call method under test... with mocked logging
        econtrol_crawler.get_data(tmp_data_path)

    # Check calls
    mock_getenv.assert_called_with("ECONTROL_AT_AUTH")
    mock_get_paginated_stations.assert_called_once()
    mock_file.assert_not_called()
    assert mock_file.write.called is False

    # Get file contents
    file_contents = mock_file.__enter__().getvalue()

    # Check logging and file size via log message
    assert f"Downloaded file size: {expected_file_size} bytes" in local_caplog.logs

    # Check file size
    assert len(file_contents) == expected_file_size

    # Check objects from file, after reading it back from the in-memory file
    # actual_file_content_objs = [json.loads(line) for line in file_contents.splitlines()]
    # expected_objs = [{f"id{i}": f"station{i}"} for i in range(1, 100 + 1)]
    # assert actual_file_content_objs == expected_objs
