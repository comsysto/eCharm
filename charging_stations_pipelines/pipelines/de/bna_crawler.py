"""Module to download the BNA (Bundesnetzagentur) data from a specified URL."""

import logging
import os
from pathlib import Path
from typing import Optional

import requests as requests
from bs4 import BeautifulSoup

from charging_stations_pipelines.file_utils import download_file
from charging_stations_pipelines.pipelines.de import DownloadFileException, ExtractURLException, FetchWebsiteException

logger = logging.getLogger(__name__)

BASE_URL = "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html"
LINK_CLASS = "downloadLink Publication FTxlsx"
SEARCH_TERM = "ladesaeulenregister"
FILE_EXTENSION = ".xlsx"
USER_AGENT = "Mozilla/5.0"
PARSER = "html.parser"


def get_bna_data(out_file: Path) -> None:
    """Downloads BNA (Bundesnetzagentur) data (Excel sheet 'Ladesaeulenregister.xlsx') from its website into a specified
    file."""
    # Base url & header
    headers = {"User-Agent": USER_AGENT}
    # Fetch the website for further processing
    response = requests.get(BASE_URL, headers=headers)

    # Check if the request has been successful
    if response.status_code != 200:
        raise FetchWebsiteException(f"Failed to fetch the website: {BASE_URL}")

    soup = BeautifulSoup(response.content, PARSER)
    # Lookup for the link in the html
    download_link_elems = soup.find_all("a", class_=LINK_CLASS)
    download_link_url: Optional[str] = None
    for link in download_link_elems:
        download_link_url = link.get('href')
        if (download_link_url
                and SEARCH_TERM in download_link_url.lower()
                and download_link_url.lower().endswith(FILE_EXTENSION)):
            break

    # Check if the url extraction is successful
    if download_link_url is None:
        raise ExtractURLException("Failed to extract the download url from the website.")

    logger.debug(f"Downloading BNA data from '{download_link_url}'")
    try:
        download_file(download_link_url, out_file)
    except Exception as ex:
        raise DownloadFileException(f"Could not download the file: {ex}")

    logger.debug(f"Downloaded data to '{out_file}'")
    logger.debug(f"Downloaded file size: {os.path.getsize(out_file)} bytes")
