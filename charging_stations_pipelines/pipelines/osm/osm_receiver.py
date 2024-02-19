"""Module to retrieve OpenStreetMap (OSM) data for a specific country"""

import json

import requests
from requests import Response

from charging_stations_pipelines.pipelines.osm import (
    DATA_SOURCE_KEY,
)


def get_osm_data(country_code: str, tmp_data_path):
    """This method retrieves OpenStreetMap (OSM) data for a specific country based on its country code. The OSM data
    includes information about charging stations in the specified country.

    The `country_code` parameter is a string representing the ISO3166-1 alpha-2 country code.

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

    query_params = {
        "data": f"""
        [out:json];
        
        area["ISO3166-1"="{country_code}"][admin_level=2];
        
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

    response: Response = requests.get("https://overpass-api.de/api/interpreter", query_params)
    status_code: int = response.status_code
    if status_code != 200:
        raise RuntimeError(f"Failed to get {DATA_SOURCE_KEY} data! Status code: {status_code}")
    with open(tmp_data_path, "w") as f:
        json.dump(response.json(), f, ensure_ascii=False, indent=4, sort_keys=True)
