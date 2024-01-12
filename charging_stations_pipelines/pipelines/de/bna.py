"""Bundesnetzagentur (BNA) Pipeline."""

import configparser
import logging
import pathlib
from typing import Final

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
from ...pipelines import Pipeline
from ...pipelines.station_table_updater import StationTableUpdater
from ...shared import load_excel_file

logger = logging.getLogger(__name__)


class BnaPipeline(Pipeline):
    def __init__(self, config: configparser, session: Session, online: bool = False):
        super().__init__(config, session, online)

        # All BNA data is from Germany
        self.country_code = "DE"

        self.data_dir: Final[pathlib.Path] = (pathlib.Path(__file__).parents[3] / "data").resolve()

    def retrieve_data(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        tmp_data_path = self.data_dir / self.config[DATA_SOURCE_KEY]["filename"]

        if self.online:
            logger.info("Retrieving Online Data")
            get_bna_data(tmp_data_path)

            logger.info(f"Loading data from file: {tmp_data_path}")
            self.data: pd.DataFrame = load_excel_file(tmp_data_path)
            logger.info(f"Finished loading data: {self.data.shape} rows!")

    def run(self):
        logger.info(f"Running {self.country_code}/{DATA_SOURCE_KEY} Pipeline...")
        self.retrieve_data()

        logger.info(f"Mapping {DATA_SOURCE_KEY} data...")
        station_updater = StationTableUpdater(session=self.session, logger=logger)
        row: pd.Series
        for _, row in tqdm(iterable=self.data.iterrows(), total=self.data.shape[0]):
            try:
                mapped_station = map_station_bna(row)
                mapped_station.address = map_address_bna(row, None)
                mapped_station.charging = map_charging_bna(row, None)
            except Exception as e:
                logger.error(f"{DATA_SOURCE_KEY} entry could not be mapped! Error: {e}")
                continue

            station_updater.update_station(mapped_station, DATA_SOURCE_KEY)

        station_updater.log_update_station_counts()
        logger.info("Finished mapping!")
        logger.info(f"Finished {self.country_code}/{DATA_SOURCE_KEY} Pipeline!")
