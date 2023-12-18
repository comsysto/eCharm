"""Module to retrieve OpenStreetMap (OSM) data for a specific country"""

import json
import logging
from pathlib import Path

import requests
from requests import Response

from charging_stations_pipelines.pipelines.osm import DATA_SOURCE_KEY, SUPPORTED_COUNTRIES

logger = logging.getLogger(__name__)


def get_osm_data(country_code: str, out_path: Path) -> None:
    """This method retrieves OpenStreetMap (OSM) data for a specific country based on its country code. The OSM data
    includes information about charging stations in the specified country.

    The `country_code` parameter is a string representing the two-letter country code. Valid country codes are "DE"
    for Germany, "FR" for France, "GB" for the United Kingdom, "IT" for Italy, "NOR" for Norway, and "SWE" for Sweden.

    The `tmp_data_path parameter` is a string representing the path to save the downloaded OSM data. The OSM data will
    be saved in JSON format.

    This method uses the Overpass API to retrieve the OSM data. It sends a query to the Overpass API specifying
    the desired country and the amenity `charging_station` to retrieve information about charging stations within
    the country.

    Example usage:
        >>> get_osm_data("DE", Path("/path/to/save/osm_data.json"))

    :param country_code: The country code of the desired country.
    :param out_path: The path to save the downloaded OSM data.
    :return: None
    """
    if country_code not in SUPPORTED_COUNTRIES:
        raise ValueError(f"Country code '{country_code}' unknown for {DATA_SOURCE_KEY}")

    query_params = {
        "data": f"""
        [out:json];

        area["ISO3166-1"="{country_code}"]["admin_level"="2"];
        
        // gather results
        (
            // query for: "charging station"
            node["amenity"="charging_station"](area);
            way["amenity"="charging_station"](area);
            rel["amenity"="charging_station"](area);
        );
        
        out;
        """
    }

    logger.info("Retrieving data from Overpass API")
    logger.debug(f"Query params / 'data': {query_params['data']}")
    response: Response = requests.get("https://overpass-api.de/api/interpreter", query_params)

    logger.debug(f"Run query: {response.url}")
    logger.debug(f"Received response size in bytes: {len(response.content)}")
    status_code: int = response.status_code
    if status_code != 200:
        raise RuntimeError(f"Failed to get {DATA_SOURCE_KEY} data! Status code: {status_code}")

    logger.debug(f"Writing data from Overpass API to file: {out_path}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as file:
        json.dump(response.json(), file, ensure_ascii=False)

    logger.info("Successfully wrote data from Overpass API")
