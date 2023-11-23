"""Module to download the e-control.at (ladestellen.at) data from a specified URL."""

import json
import logging
import os

import requests

from charging_stations_pipelines.pipelines.at import DATA_SOURCE_KEY

logger = logging.getLogger(__name__)


def _get_paginated_stations(url, headers):
    session = requests.Session()
    session.headers.update(headers)

    first_page = session.get(url).json()

    try:
        # Sample data from returned JSON chunk: "totalResults":9454,"fromIndex":0,"endIndex":999
        total_count = first_page['totalResults']
        logger.info(f"Total count of stations: {total_count}")
        yield first_page

        idx_start = first_page['fromIndex']
        idx_end = first_page['endIndex']
    except KeyError as e:
        logging.fatal(f'Failed to parse response:\n{first_page}\n{e}')
        raise e

    # number of datapoints (=station) per page, e.g. 1000
    page_size = idx_end - idx_start + 1  # Final[int]

    if total_count <= page_size:
        # no paginagion needed
        return

    num_pages = total_count // page_size + 1
    for page_num in range(2, num_pages + 1):
        idx_start = page_size * (page_num - 1)
        idx_end = page_size * page_num - 1

        logger.debug(f'Downloading chunk: {idx_start}..{idx_end}')
        next_page = (session
                     .get(url, params={'fromIndex': idx_start, 'endIndex': idx_end})
                     .json())
        yield next_page


def get_data(tmp_data_path):
    """Downloads data from a specified URL and saves it to a file.

    :param tmp_data_path: The path to the file where the data will be saved.
    :type tmp_data_path: str
    :return: None
    :rtype: None
    """
    url = "https://api.e-control.at/charge/1.0/search/stations"  # Final[str]

    # HTTP header
    #
    # TODO fix the issue with the api key
    # econtrol_at_apikey = os.getenv('ECONTROL_AT_APIKEY')
    # econtrol_at_domain = os.getenv('ECONTROL_AT_DOMAIN')
    headers = {'Authorization': f"Basic {os.getenv('ECONTROL_AT_AUTH')}", 'User-Agent': 'Mozilla/5.0'}
    logger.critical(f'Using HTTP headers:\n{headers}')

    logger.info(f"Downloading {DATA_SOURCE_KEY} data from {url}...")
    with open(tmp_data_path, 'w') as f:
        for page in _get_paginated_stations(url, headers):
            logger.debug(f"Getting data: {page['fromIndex']}..{page['endIndex']}")

            # save as newline-delimited JSON (*.ndjson), i.e. one JSON object per line
            for station in page['stations']:
                json.dump(station, f, ensure_ascii=False)
                f.write('\n')
    logger.info(f"Downloaded {DATA_SOURCE_KEY} data to: {tmp_data_path}")
    logger.info(f"Downloaded file size: {os.path.getsize(tmp_data_path)} bytes")


if __name__ == '__main__':
    from dotenv import load_dotenv

    load_dotenv()

    logger.setLevel(logging.DEBUG)

    get_data('/tmp/stations.ndjson')
