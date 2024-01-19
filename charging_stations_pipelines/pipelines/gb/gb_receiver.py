"""Crawler for GB station data."""

import json

import requests
from requests import Response


def get_gb_data(tmp_file_path):
    """Retrieves data from the GB-Gov-Data API and writes it to a temporary file.
    See https://chargepoints.dft.gov.uk/api/help."""
    api_url = "https://chargepoints.dft.gov.uk/api/retrieve/registry/format/json/"
    response: Response = requests.get(api_url)
    response.json()

    status_code: int = response.status_code
    if status_code != 200:
        raise RuntimeError(f"Failed to get GB-Gov-Data! Status-Code: {status_code}")
    with open(tmp_file_path, "w") as f:
        json.dump(response.json(), f, ensure_ascii=False, indent=4, sort_keys=True)
