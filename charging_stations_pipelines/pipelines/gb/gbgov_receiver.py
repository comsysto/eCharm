"""Crawler for GB station data."""

import json
from pathlib import Path
from typing import Final

import requests
from requests import Response

from . import DATA_SOURCE_KEY


def get_gb_data(out_file: Path) -> None:
    """Retrieves data from the GB-Gov-Data API and writes it to a temporary file.
        See https://chargepoints.dft.gov.uk/api/help."""
    api_url: Final[str] = "https://chargepoints.dft.gov.uk/api/retrieve/registry/format/json/"
    response: Response = requests.get(api_url)

    status_code: int = response.status_code
    if status_code != 200:
        raise RuntimeError(f"Failed to get {DATA_SOURCE_KEY} data! status code: {status_code}")

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w") as file:
        json.dump(response.json(), file, ensure_ascii=False, indent=4, sort_keys=True)
