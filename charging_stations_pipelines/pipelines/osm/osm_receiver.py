import json

import requests
from requests import Response


def get_osm_data(country_code: str, tmp_data_path):
    country_code_to_area = {
`        # TODO: Add all EU countries
        "DE": "Deutschland",
        "AT": "Österreich",
        "FR": "France métropolitaine",
        "GB": "United Kingdom",
        "IT": "Italia",
        "NOR": "Norge",
        "SWE": "Sverige"
    }

    if country_code not in country_code_to_area:
        raise Exception(f"country code '{country_code}' unknown for OSM")

    area_name = country_code_to_area[country_code]

    query_params = {
        "data": """
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
        """.format(area_name=area_name)
    }

    response: Response = requests.get(
        "http://overpass-api.de/api/interpreter", params=query_params
    )
    status_code: int = response.status_code
    if status_code != 200:
        raise RuntimeError(f"Failed to get OSM-Data! Status-Code: {status_code}")
    with open(tmp_data_path, "w") as f:
        json.dump(response.json(), f, ensure_ascii=False, indent=4, sort_keys=True)
