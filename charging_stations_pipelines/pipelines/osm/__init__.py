"""The OpenStreetMap (OSM) data source pipeline."""
from typing import Final

from typing_extensions import KeysView

DATA_SOURCE_KEY: Final[str] = "OSM"
"""The data source key for the OpenStreetMap (OSM) data source."""

COUNTRY_CODE_TO_AREA_MAP: Final[dict[str, str]] = {
    "DE": "Deutschland",
    "AT": "Österreich",
    "FR": "France métropolitaine",
    "GB": "United Kingdom",
    "IT": "Italia",
    "NOR": "Norge",
    "SWE": "Sverige",
}
"""The mapping of country codes to the corresponding area names for the OSM API."""

SCOPE_COUNTRIES: Final[KeysView[str]] = COUNTRY_CODE_TO_AREA_MAP.keys()
"""The list of country codes covered by the AT data source."""
