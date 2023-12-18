"""Unit tests for the crawler of the BNA pipeline."""
import logging
import os
from unittest.mock import Mock, patch

import pytest
import requests

import charging_stations_pipelines.pipelines.de.bna_crawler
from charging_stations_pipelines.pipelines.de import bna_crawler
from charging_stations_pipelines.pipelines.de.bna_crawler import __name__ as test_module_name
# NOTE: "local_caplog" is a pytest fixture from test.shared.local_caplog
from test.shared import local_caplog, LogLocalCaptureFixture  # noqa: F401


@patch.object(charging_stations_pipelines.pipelines.de.bna_crawler, 'BeautifulSoup')
@patch.object(charging_stations_pipelines.pipelines.de.bna_crawler, 'download_file')
@patch.object(requests, 'get')
@patch.object(os.path, 'getsize')
def test_get_bna_data_downloads_file_with_correct_url(mock_getsize, mock_requests_get, mock_download_file,
                                                      mock_beautiful_soup):
    # Mock the requests.get response
    mock_response = Mock()
    mock_response.content = b'something, something...'
    mock_response.status_code = 200
    mock_requests_get.return_value = mock_response

    # Mock the BeautifulSoup find_all method
    mock_beautiful_soup.return_value.find_all.return_value = [{'href': 'https://some_ladesaeulenregister_url.xlsx'}]

    # Mock the os.path.getsize method
    mock_getsize.return_value = 4321

    # Call the method under test
    bna_crawler.get_bna_data('./tmp_data_path/some_ladesaeulenregister_url.xlsx')

    # Ensure these function were called with the expected arguments.
    mock_requests_get.assert_called_with(
            "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html",
            headers={"User-Agent": "Mozilla/5.0"})

    # Assert that the download_file method was called with the correct parameters
    mock_download_file.assert_called_once_with(
            'https://some_ladesaeulenregister_url.xlsx', './tmp_data_path/some_ladesaeulenregister_url.xlsx')

    # Assert that the os.path.getsize method was called with the correct parameters
    mock_getsize.assert_called_once_with('./tmp_data_path/some_ladesaeulenregister_url.xlsx')


@patch.object(requests, 'get')
@patch.object(charging_stations_pipelines.pipelines.de.bna_crawler, 'BeautifulSoup')
def test_get_bna_data_logs_error_when_no_download_link_found(mock_beautiful_soup, mock_requests_get, caplog):
    # Mock the requests.get response
    mock_requests_get.return_value = Mock(content=b'some content', status_code=200)

    # Mock the BeautifulSoup find method to return None
    mock_beautiful_soup.return_value.find_all.return_value = []

    with pytest.raises(bna_crawler.ExtractURLException, match='Failed to extract the download url from the website.'):
        # Call the function under test
        bna_crawler.get_bna_data('tmp_data_path')


@patch.object(requests, 'get')
@patch.object(charging_stations_pipelines.pipelines.de.bna_crawler, 'BeautifulSoup')
@patch.object(charging_stations_pipelines.pipelines.de.bna_crawler, 'download_file')
@patch.object(os.path, 'getsize')
def test_get_bna_data_logs_file_size_after_download(mock_getsize, mock_download_file, mock_beautiful_soup,
                                                    mock_requests_get, local_caplog: LogLocalCaptureFixture):
    # Mock the requests.get response
    mock_requests_get.return_value = Mock(content=b'some content')
    mock_requests_get.return_value.status_code = 200

    # Mock the BeautifulSoup find_all method
    mock_beautiful_soup.return_value.find_all.return_value = [
        {'href': 'some_url_without_search_term.xlsx'},
        {'href': 'tmp_data_path/ladesaeulenregister.xlsx'}
    ]

    # Mock the os.path.getsize method
    mock_getsize.return_value = 1234

    logger = logging.getLogger(test_module_name)
    with local_caplog(level=logging.DEBUG, logger=logger):
        # Call method under test... with mocked logging
        bna_crawler.get_bna_data('tmp_data_path/some_url1_with_search_term.xlsx')
