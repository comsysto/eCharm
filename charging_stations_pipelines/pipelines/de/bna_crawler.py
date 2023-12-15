"""Module to download the BNA (Bundesnetzagentur) data from a specified URL."""

import logging
import os
from pathlib import Path

import requests as requests
from bs4 import BeautifulSoup

from charging_stations_pipelines.file_utils import download_file

logger = logging.getLogger(__name__)


def get_bna_data(out_file: Path) -> None:
    """Downloads BNA (Bundesnetzagentur) data (Excel sheet 'ladesaeulenregister.xlsx') from its website into
    a specified file.
    """
    # Base url & header
    headers = {"User-Agent": "Mozilla/5.0"}
    base_url = "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html"

    r = requests.get(base_url, headers=headers)
    soup = BeautifulSoup(r.content, "html.parser")

    # Lookup for the link in the html
    # Example URL from "href" attribute:
    #   https://data.bundesnetzagentur.de/Bundesnetzagentur/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/ladesaeulenregister.xlsx
    download_link_elem = soup.find("a", class_="FTxlsx")
    download_link_url = download_link_elem.get("href")

    logger.info(f"Downloading data from {download_link_url}...")
    download_file(download_link_url, out_file)

    logger.debug(f"Downloaded data to {out_file}")
    logger.debug(f"Downloaded file size: {os.path.getsize(out_file)} bytes")
