import json

import requests
from requests import Response


def get_osm_data(country_code: str, tmp_data_path):
    if country_code == "DE":
        area_name = "Deutschland"
    elif country_code == "FR":
        area_name = "France métropolitaine"
    elif country_code == "GB":
        area_name = "United Kingdom"
    else:
        raise Exception(f"country code '{country_code}' unknown for OSM")

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

if __name__ == "__main__":
    get_osm_data(tmp_data_path="./osm_france.json")