"""Test the AT pipeline crawler with mock tests."""

import json
import unittest
from unittest.mock import call, patch

from charging_stations_pipelines.pipelines.at.econtrol_crawler import _get_paginated_stations, get_data


@patch('requests.Session.get')
def test_paginated_stations(mock_get):
    station_api_url = 'https://api.test-charge.com/stations'
    station_pages = [{'totalResults': 13, 'fromIndex': i * 3, 'endIndex': min((i + 1) * 3 - 1, 13 - 1),
                      'stations':     [{f'id{j}': f'station{j}'} for j in range(i * 3 + 1, (i + 1) * 3)]}
                     for i in range(13 // 3 + 13 % 3)]

    page_generator = _get_paginated_stations(station_api_url, headers={})

    for idx, station_page in enumerate(station_pages):
        mock_get.return_value.json.return_value = station_page
        assert next(page_generator) == station_page
        mock_get.assert_called_with(
                station_api_url,
                params={'fromIndex': station_page['fromIndex'],
                        'endIndex':  station_page[
                                         'endIndex']}) if idx else mock_get.assert_called_with(station_api_url)

    assert mock_get.call_count == len(station_pages)


@patch('builtins.open', new_callable=unittest.mock.mock_open)
@patch('charging_stations_pipelines.pipelines.at.econtrol_crawler._get_paginated_stations')
@patch('os.getenv')
def test_get_data(mock_getenv, mock_get_paginated_stations, mock_open):
    tmp_data_path = '/tmp/test_data.ndjson'
    mock_getenv.return_value = 'test_token'
    mock_get_paginated_stations.return_value = iter([{
        'totalResults': 100, 'fromIndex': i * 10, 'endIndex': min((i + 1) * 10 - 1, 100 - 1),
        'stations':     [{f'id{j}': f'station{j}'} for j in range(i * 10 + 1, (i + 1) * 10 + 1)]} for i in
        range(100 // 10 + 100 % 10)])

    get_data(tmp_data_path)

    mock_getenv.assert_called_with('ECONTROL_AT_AUTH')
    mock_get_paginated_stations.assert_called()

    # FIXME mock_open.assert_called_with(tmp_data_path, 'w')
    # >       mock_open.assert_called_with(tmp_data_path, 'w')
    # E       AssertionError: expected call not found.
    # E       Expected: open('/tmp/test_data.ndjson', 'w')
    # E       Actual: open('/home/vmx/.netrc')
    # E
    # E       pytest introspection follows:
    # E
    # E       Args:
    # E       assert ('/home/vmx/.netrc',) == ('/tmp/test_data.ndjson', 'w')
    # E         At index 0 diff: '/home/vmx/.netrc' != '/tmp/test_data.ndjson'
    # E         Right contains one more item: 'w'

    assert ([json.loads(line) for line in
             ''.join([kall.args[0] for kall in mock_open().write.call_args_list]).splitlines()]
            ==
            [{f'id{i}': f'station{i}'} for i in range(1, 100 + 1)])
