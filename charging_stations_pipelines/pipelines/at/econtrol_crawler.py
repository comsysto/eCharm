"""Module to download the e-control.at (ladestellen.at) data from a specified URL."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Final, Generator

import requests

from charging_stations_pipelines.pipelines import at

logger = logging.getLogger(__name__)


def get_paginated_stations(url: str, headers: dict[str, str] = None) -> Generator[dict[str, Any], None, None]:
    session = requests.Session()
    session.headers.update(headers)

    first_page = session.get(url).json()
    if not first_page:
        yield {}

    try:
        # Sample data from returned JSON chunk: "totalResults":9454,"fromIndex":0,"endIndex":999
        total_count = first_page['totalResults']
        logger.info(f"Total count of stations: {total_count}")
        yield first_page

        idx_start = first_page['fromIndex']
        idx_end = first_page['endIndex']
    except KeyError as ex:
        logging.fatal(f"Error: {ex}. Failed to parse response: '{first_page}'")
        raise ex

    # Number of datapoints (=station) per page, e.g. 1000
    page_size: Final[int] = idx_end - idx_start + 1

    if total_count <= page_size:
        # No pagination needed
        yield

    num_pages = total_count // page_size + (1 if total_count % page_size else 0)
    for page_num in range(2, num_pages + 1):
        idx_start = page_size * (page_num - 1)
        idx_end = min(page_size * page_num - 1, total_count - 1)

        logger.debug(f'Downloading chunk: {idx_start}..{idx_end}')
        next_page = session.get(url, params={'fromIndex': idx_start, 'endIndex': idx_end}).json()
        yield next_page


def get_data(out_file: Path) -> None:
    """Downloads data from a specified URL and saves it to a file."""
    url: Final[str] = 'https://api.e-control.at/charge/1.0/search/stations'

    # HTTP header
    # TODO fix the issue with the api key
    # econtrol_at_apikey = os.getenv('ECONTROL_AT_APIKEY')
    # econtrol_at_domain = os.getenv('ECONTROL_AT_DOMAIN')
    headers = {'Authorization': f"Basic {os.getenv('ECONTROL_AT_AUTH')}", 'User-Agent': 'Mozilla/5.0'}
    logger.debug(f'Using HTTP headers: {headers}')

    logger.debug(f"Downloading {at.DATA_SOURCE_KEY} data from {url}...")
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w") as file:
        for page in get_paginated_stations(url, headers):
            logger.debug(f"Getting data: {page['fromIndex']}..{page['endIndex']}")

            # Save as newline-delimited JSON (*.ndjson), i.e. one JSON object per line
            for station in page['stations']:
                json.dump(station, file, ensure_ascii=False)
                file.write('\n')
    logger.debug(f"Downloaded {at.DATA_SOURCE_KEY} data to: {out_file}")
    logger.debug(f"Downloaded file size: {out_file.stat().st_size} bytes")
