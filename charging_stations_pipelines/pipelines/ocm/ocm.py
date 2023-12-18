"""This module contains the OCM Pipeline."""

import configparser
import json
import logging
from pathlib import Path
from typing import Final

from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.pipelines.station_table_updater import StationTableUpdater
from charging_stations_pipelines.shared import JSON
from . import DATA_SOURCE_KEY, OCM_IMPORT_DATA_PATH, OCM_STAGE1_DATA_PATH, OCM_STAGE2_DATA_PATH
from .ocm_extractor import merge_ocm_pois
from .ocm_fetcher import export_ocm_rawdata, fetch_ocm_rawdata
from .ocm_mapper import map_address_ocm, map_charging_ocm, map_station_ocm
from .. import Pipeline
from ...file_utils import append_country_code_to_file_name, is_data_present

logger = logging.getLogger(__name__)


class OcmPipeline(Pipeline):
    def __init__(self, country_code: str, config: configparser, session: Session, online=False):
        super().__init__(config, session, online)

        self.country_code = country_code.upper()

        self.data_path: Final[Path] = OCM_IMPORT_DATA_PATH / append_country_code_to_file_name(
                self.config[DATA_SOURCE_KEY]["filename"], self.country_code)
        self.data_path.parent.mkdir(parents=True, exist_ok=True)

    def retrieve_data(self, **kwargs) -> None:
        if 'force_data_reload' not in kwargs:
            kwargs['force_data_reload'] = False

        is_data_reload_triggered = False

        # -> Fetch POIs and reference data from OCM endpoint
        if self.online and (not is_data_present(OCM_STAGE1_DATA_PATH) or kwargs['force_data_reload']):
            logger.debug(f"Retrieving {DATA_SOURCE_KEY}/{self.country_code} Online Data")
            fetch_ocm_rawdata(OCM_STAGE1_DATA_PATH, True)
            is_data_reload_triggered = True

        # -> Extract POIs into per-country folders
        if not is_data_present(OCM_STAGE2_DATA_PATH) or is_data_reload_triggered:
            logger.debug(f"Exporting {DATA_SOURCE_KEY}/{self.country_code} POIs data on per-country basis")
            export_ocm_rawdata(OCM_STAGE1_DATA_PATH, OCM_STAGE2_DATA_PATH)
            is_data_reload_triggered = True

        # -> Merge extracted POIs data with reference data
        if not (self.data_path.exists() and self.data_path.stat().st_size > 0) or is_data_reload_triggered:
            logger.debug(f"Merging {DATA_SOURCE_KEY}/{self.country_code} POIs data "
                         f"for the country '{self.country_code}'")
            merge_ocm_pois(OCM_STAGE2_DATA_PATH, self.data_path, self.country_code)

        # <- Load normalized POIs data
        if self.data_path.exists() and self.data_path.stat().st_size > 0:
            with self.data_path.open() as file:
                self.data = json.load(file)
                logger.debug(f"Loaded {DATA_SOURCE_KEY}/{self.country_code} data from {self.data_path}")
        else:
            self.data = {}
            logger.warning(f"No '{self.country_code}' data available in {DATA_SOURCE_KEY} data!")

    def run(self, **kwargs) -> None:
        if 'force_data_reload' not in kwargs:
            kwargs['force_data_reload'] = False

        logger.info(f"Running {DATA_SOURCE_KEY}/{self.country_code} Pipeline...")
        self.retrieve_data(force_data_reload=kwargs['force_data_reload'])

        logger.debug(f"Mapping {DATA_SOURCE_KEY}/{self.country_code} data and writing it into DB")
        station_updater = StationTableUpdater(session=self.session, logger=logger)
        entry: JSON 
        for _, entry in tqdm(iterable=iter(self.data.items()), total=len(self.data),
                             desc="Mapping entities and writing data into DB"):
            station = map_station_ocm(entry, self.country_code)
            station.address = map_address_ocm(entry, self.country_code, None)
            station.charging = map_charging_ocm(entry, None)
            station_updater.update_station(station, DATA_SOURCE_KEY)
        station_updater.log_update_station_counts()
        logger.info(f"Finished {DATA_SOURCE_KEY}/{self.country_code} Pipeline")
