"""The OpenStreetMap (OSM) data source pipeline."""

from pathlib import Path
from typing import Final, KeysView

from ... import EUROPEAN_COUNTRIES, PROJ_DATA_DIR

DATA_SOURCE_KEY: Final[str] = "OSM"
"""The data source key for the OpenStreetMap (OSM) data source."""

SUPPORTED_COUNTRIES: Final[KeysView[str]] = EUROPEAN_COUNTRIES.keys()
"""The list of country codes covered by the OSM data source."""

OSM_DATA_PATH: Final[Path] = PROJ_DATA_DIR / "osm"
"""The path to the OSM data folder."""
