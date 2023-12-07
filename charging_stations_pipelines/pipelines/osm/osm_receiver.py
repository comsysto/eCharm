"""Module to retrieve OpenStreetMap (OSM) data for a specific country"""

import json

import requests
from requests import Response

from charging_stations_pipelines.pipelines.osm import (
    COUNTRY_CODE_TO_AREA_MAP,
    DATA_SOURCE_KEY,
)


def get_osm_data(country_code: str, tmp_data_path):
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
        >>> get_osm_data("DE", "/path/to/save/osm_data.json")

    :param country_code: The country code of the desired country.
    :type country_code: str
    :param tmp_data_path: The path to save the downloaded OSM data.
    :type tmp_data_path: str
    :return: None
    """
    if country_code not in COUNTRY_CODE_TO_AREA_MAP:
        raise Exception(f"country code '{country_code}' unknown for {DATA_SOURCE_KEY}")

    area_name = COUNTRY_CODE_TO_AREA_MAP[country_code]

    query_params = {
        "data": f"""
        [out:json];
        
        area[name="{area_name}"];
        
        // gather results
        (
            // query part for: “"charging station"”
            node["amenity"="charging_station"](area);
            way["amenity"="charging_station"](area);
            rel["amenity"="charging_station"](area);
        );
        
        out;
        """
    }

    response: Response = requests.get(
        "https://overpass-api.de/api/interpreter", query_params
    )
    status_code: int = response.status_code
    if status_code != 200:
        raise RuntimeError(
            f"Failed to get {DATA_SOURCE_KEY} data! Status code: {status_code}"
        )
    with open(tmp_data_path, "w") as f:
        json.dump(response.json(), f, ensure_ascii=False, indent=4, sort_keys=True)
