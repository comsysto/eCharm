"""This module contains the GB GOV Pipeline."""

import configparser
import json
import logging
from pathlib import Path
from typing import Final

from sqlalchemy.orm import Session

from charging_stations_pipelines import PROJ_DATA_DIR
from charging_stations_pipelines.pipelines import Pipeline
from charging_stations_pipelines.pipelines.station_table_updater import (
    StationTableUpdater,
)
from charging_stations_pipelines.shared import JSON
from . import DATA_SOURCE_KEY
from .gbgov_mapper import map_address_gb, map_charging_gb, map_station_gb
from .gbgov_receiver import get_gb_data

logger = logging.getLogger(__name__)


class GbPipeline(Pipeline):
    def __init__(self, config: configparser, session: Session, online: bool = False):
        super().__init__(config, session, online)

        # All data is from the UK
        self.country_code: Final[str] = "GB"

        self.data_path: Final[Path] = PROJ_DATA_DIR / self.config[DATA_SOURCE_KEY]["filename"]
        self.data_path.parent.mkdir(parents=True, exist_ok=True)

    def retrieve_data(self):
        if self.online:
            logger.info("Retrieving Online Data")
            get_gb_data(self.data_path)

        with self.data_path.open() as file:
            self.data = json.load(file)

    def run(self):
        logger.info(f"Running {DATA_SOURCE_KEY} Pipeline...")
        self.retrieve_data()

        station_updater = StationTableUpdater(session=self.session, logger=logger)
        entry: JSON
        for entry in self.data.get("ChargeDevice", []):
            mapped_station = map_station_gb(entry, self.country_code)
            mapped_station.address = map_address_gb(entry, None)
            mapped_station.charging = map_charging_gb(entry)
            station_updater.update_station(mapped_station, DATA_SOURCE_KEY)
        station_updater.log_update_station_counts()
