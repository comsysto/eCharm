"""OCM data export from https://api-01.openchargemap.io/v3/poi and https://api-01.openchargemap.io/v3/referencedata."""

import json
import logging
import time
from pathlib import Path
from typing import Final

import math
import requests
from tqdm import tqdm

from charging_stations_pipelines.shared import JSON
from . import EUROPEAN_COUNTRIES
from ...file_utils import create_success_marker_file

OCM_API_URL: Final[str] = "https://api-01.openchargemap.io/v3"

HEADERS: Final[dict[str, str]] = {'User-Agent': 'curl/8.5.0'}

# Estimated size of the files to download in case the content-length header is not found
EST_FILE_SIZE: Final[dict[str, int]] = {
    'poi.json':           200_000_000,
    'referencedata.json': 400_000
}


logger = logging.getLogger(__name__)


def get_country_code(poi: JSON, reference_data: JSON) -> str:
    """Returns the country code for the given POI."""
    return next((c.get("ISOCode") for c in reference_data.get('Countries')
                 if c.get("ID") == poi.get('AddressInfo').get('CountryID')), None)


def export_pois_for_country(poi: JSON, country_code: str, out_dir: Path) -> None:
    """Exports the given POI to the data folder."""
    poi_data_out_file: Path = out_dir / country_code / f'OCM-{poi.get("ID")}.json'

    poi_data_out_file.parent.mkdir(parents=True, exist_ok=True)
    with poi_data_out_file.open("w") as file:
        json.dump(poi, file, ensure_ascii=False)


def export_pois_data(pois_data_in_file: Path, refs_data_in_file: Path, out_dir: Path) -> None:
    """Exports the POI data to the data folder, grouped by country."""
    start_time = time.time()

    with pois_data_in_file.open() as file:
        pois_data: JSON = json.load(file)

    with refs_data_in_file.open() as file:
        refs_data: JSON = json.load(file)

    logger.debug('Exporting POIs to data folder...')
    counter = 0
    with tqdm(total=len(pois_data), desc="Exporting POIs") as pbar:
        poi: JSON
        for poi in pois_data:
            country_code: str = get_country_code(poi, refs_data)
            if country_code in EUROPEAN_COUNTRIES:
                counter += 1
                export_pois_for_country(poi, country_code, out_dir)
            pbar.update()

    logger.debug(f'Completed export: {counter} POIs exported.')

    refs_data_out_file = out_dir / 'referencedata.json'

    refs_data_out_file.parent.mkdir(parents=True, exist_ok=True)
    with refs_data_out_file.open("w") as file:
        json.dump(refs_data, file, ensure_ascii=False)
    # After export has finished, place success marker file inside the output data folder
    create_success_marker_file(out_dir)

    logger.debug(f"Extraction completed in {math.ceil(time.time() - start_time)} seconds.")


def download_with_progressbar(url: str, out_file: Path) -> None:
    """Downloads the file from the given URL and saves it to the given output path."""
    start_time = time.time()
    response = requests.get(url, headers=HEADERS, stream=True)

    total_size = response.headers.get('content-length')
    if total_size is None:
        total_size = EST_FILE_SIZE.get(out_file.name, 1)
        logger.debug(f"Content-length header not found; total_size set to a default value of {total_size} bytes.")
    else:
        total_size = int(total_size)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("wb") as file, tqdm(total=total_size, unit='B', unit_scale=True, desc=str(out_file)) as pbar:
        for data in response.iter_content(chunk_size=1024):
            file.write(data)
            pbar.update(len(data))

    end_time = time.time()
    logger.debug(f"Download completed in {math.ceil(end_time - start_time)} seconds.")


def download_ocm_data(poi_data_out_file: Path, reference_data_out_file: Path) -> None:
    """Downloads the latest OCM data (POIs) from the OCM API."""
    # Downloading the latest poi.json data from OCM
    poi_url: str = (f"{OCM_API_URL}/poi"
                    "?client=ocm-data-export"
                    "&maxresults=500000"
                    "&compact=true"
                    "&verbose=false"
                    "&includecomments=false"
                    "&excludecomputed=true")
    logger.debug(f"Downloading '{poi_url}'")
    download_with_progressbar(poi_url, poi_data_out_file)

    # Downloading the latest referencedata.json data from OCM
    referencedata_url: str = (f"{OCM_API_URL}/referencedata"
                              f"?client=ocm-data-export")
    logger.debug(f"Downloading '{referencedata_url}'")
    download_with_progressbar(referencedata_url, reference_data_out_file)


def fetch_ocm_rawdata(out_dir: Path, should_use_cached_data=False):
    """Fetches the latest OCM data (POIs) from the OCM API."""
    start_time = time.time()

    poi_data_out_file = out_dir / 'poi.json'
    reference_data_out_file = out_dir / 'referencedata.json'

    # Downloading the latest poi.json data from OCM
    if not should_use_cached_data:
        logger.debug("Fetching OCM POI data...")
        download_ocm_data(poi_data_out_file, reference_data_out_file)
    elif poi_data_out_file.exists() and reference_data_out_file.exists():
        logger.debug("poi.json and referencedata.json already exists, skipping download.")
    else:
        logger.debug("Neither poi.json nor referencedata.json exist. Fetching OCM POI data...")
        download_ocm_data(poi_data_out_file, reference_data_out_file)

    # After downloads have finished, place success marker file inside the output data folder
    create_success_marker_file(out_dir)

    logger.debug(f"Completed in {math.ceil(time.time() - start_time)} seconds.")


def export_ocm_rawdata(in_dir: Path, out_dir: Path):
    """Fetches the latest OCM data (POIs) from the OCM API and exports it to the data folder."""
    start_time = time.time()

    poi_data_in_file = in_dir / 'poi.json'
    reference_data_in_file = in_dir / 'referencedata.json'

    # Explode POIs data into per-country folders
    export_pois_data(poi_data_in_file, reference_data_in_file, out_dir)

    logger.debug(f"Completed in {math.ceil(time.time() - start_time)} seconds.")
