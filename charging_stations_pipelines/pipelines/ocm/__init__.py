"""The Open Charge Map (OCM) data source."""

from typing import Final

from typing_extensions import KeysView

from ... import EUROPEAN_COUNTRIES, PROJ_DATA_DIR

DATA_SOURCE_KEY: Final[str] = "OCM"
"""The data source key for the OpenChargeMap (OCM) data source."""

SCOPE_COUNTRIES: Final[KeysView[str]] = EUROPEAN_COUNTRIES.keys()
"""The list of countries covered by the OCM data source."""

OCM_DATA_PATH = PROJ_DATA_DIR / DATA_SOURCE_KEY.lower()
"""The path to the OCM data folder."""

OCM_IMPORT_DATA_PATH = OCM_DATA_PATH / '03_import'
"""3. Date ready to be imported into the database by the mapper."""

OCM_STAGE1_DATA_PATH = (OCM_IMPORT_DATA_PATH.parent / '01_stage').resolve()
"""1. poi.json and referencedata.json fetched from the OCM API."""

OCM_STAGE2_DATA_PATH = (OCM_IMPORT_DATA_PATH.parent / '02_stage').resolve()
"""2. POIs grouped by country and merged referencedata.json."""
