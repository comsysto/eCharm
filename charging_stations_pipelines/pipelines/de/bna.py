"""Bundesnetzagentur (BNA) Pipeline."""

import configparser
import logging
import os
import pathlib

import pandas as pd
from sqlalchemy.orm import Session
from tqdm import tqdm

import charging_stations_pipelines.pipelines.de as de
from charging_stations_pipelines.pipelines import Pipeline
from charging_stations_pipelines.pipelines.de.bna_crawler import get_bna_data
from charging_stations_pipelines.pipelines.de.bna_mapper import (
    map_address_bna,
    map_charging_bna,
    map_station_bna,
)
from charging_stations_pipelines.pipelines.station_table_updater import (
    StationTableUpdater,
)
from charging_stations_pipelines.shared import load_excel_file

logger = logging.getLogger(__name__)


class BnaPipeline(Pipeline):
    def __init__(self, config: configparser, session: Session, online: bool = False):
        super().__init__(config, session, online)

        relative_dir = os.path.join("../../..", "data")
        self.data_dir = os.path.join(
            pathlib.Path(__file__).parent.resolve(), relative_dir
        )

    def retrieve_data(self):
        pathlib.Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        tmp_data_path = os.path.join(
            self.data_dir, self.config[de.DATA_SOURCE_KEY]["filename"]
        )
        if self.online:
            logger.info("Retrieving Online Data")
            get_bna_data(tmp_data_path)
        self.data = load_excel_file(tmp_data_path)

    def run(self):
        logger.info("Running DE GOV Pipeline...")
        self.retrieve_data()
        station_updater = StationTableUpdater(session=self.session, logger=logger)

        row: pd.Series
        for _, row in tqdm(iterable=self.data.iterrows(), total=self.data.shape[0]):
            try:
                mapped_address = map_address_bna(row, None)
                mapped_charging = map_charging_bna(row, None)

                mapped_station = map_station_bna(row)

                mapped_station.address = mapped_address
                mapped_station.charging = mapped_charging
            except Exception as e:
                logger.error(f"{de.DATA_SOURCE_KEY} entry could not be mapped! Error: {e}")
                continue
            station_updater.update_station(mapped_station, de.DATA_SOURCE_KEY)
        station_updater.log_update_station_counts()
