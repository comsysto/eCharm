"""Bundesnetzagentur (BNA) Pipeline."""

import configparser
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session
from tqdm import tqdm

from . import DATA_SOURCE_KEY
from .bna_crawler import get_bna_data
from .bna_mapper import (
    map_address_bna,
    map_charging_bna,
    map_station_bna,
)
from ... import PROJ_DATA_DIR
from ...file_utils import load_excel_file
from ...pipelines import Pipeline
from ...pipelines.station_table_updater import StationTableUpdater

logger = logging.getLogger(__name__)


class BnaPipeline(Pipeline):
    def __init__(self, config: configparser, session: Session, online: bool = False):
        super().__init__(config, session, online)

        # All BNA data is from Germany
        self.country_code = "DE"

        self.data_path: Path = PROJ_DATA_DIR / self.config[DATA_SOURCE_KEY]["filename"]
        self.data_path.parent.mkdir(parents=True, exist_ok=True)

    def retrieve_data(self):
        if self.online:
            logger.debug("Retrieving online data")
            get_bna_data(self.data_path)
            logger.debug(f"Loading data from file: {self.data_path}")
            self.data: pd.DataFrame = load_excel_file(self.data_path)
            logger.debug(f"Finished loading data: {self.data.shape} rows!")

    def run(self):
        logger.info(f"Running {self.country_code}/{DATA_SOURCE_KEY} Pipeline...")
        self.retrieve_data()

        logger.debug(f"Mapping {DATA_SOURCE_KEY} data...")
        station_updater = StationTableUpdater(session=self.session, logger=logger)
        row: pd.Series
        for _, row in tqdm(iterable=self.data.iterrows(), total=self.data.shape[0]):
            try:
                mapped_station = map_station_bna(row, self.country_code)
                mapped_station.address = map_address_bna(row, self.country_code, None)
                mapped_station.charging = map_charging_bna(row, None)
            except Exception as e:
                logger.error(f"{DATA_SOURCE_KEY} entry could not be mapped! Error: {e}")
                continue

            station_updater.update_station(mapped_station, DATA_SOURCE_KEY)

        station_updater.log_update_station_counts()
        logger.info(f"Finished {self.country_code}/{DATA_SOURCE_KEY} Pipeline!")
