"""This module contains the OCM Pipeline."""

import configparser
import json
import logging
import os
import pathlib

from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.pipelines import Pipeline
from charging_stations_pipelines.pipelines.ocm.ocm_extractor import ocm_extractor
from charging_stations_pipelines.pipelines.ocm.ocm_mapper import map_address_ocm, map_charging_ocm, map_station_ocm
from charging_stations_pipelines.pipelines.station_table_updater import StationTableUpdater
from charging_stations_pipelines.shared import JSON

logger = logging.getLogger(__name__)


class OcmPipeline(Pipeline):
    def __init__(self, country_code: str, config: configparser, session: Session, online: bool = False):
        super().__init__(config, session, online)

        self.country_code = country_code
        self.data: JSON = None

    def _retrieve_data(self):
        data_dir: str = os.path.join(
                pathlib.Path(__file__).parent.resolve(), "../../..", "data"
        )
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_file_path = os.path.join(data_dir, self.config["OCM"]["filename"])
        if self.online:
            logger.info("Retrieving Online Data")
            ocm_extractor(tmp_file_path, self.country_code)
        with open(tmp_file_path) as f:
            self.data = json.load(f)

    def run(self):
        logger.info(f"Running {self.country_code} OCM Pipeline...")
        self._retrieve_data()
        station_updater = StationTableUpdater(session=self.session, logger=logger)

        entry: JSON
        for _, entry in tqdm(iterable=iter(self.data.items()), total=len(self.data)):
            mapped_address = map_address_ocm(entry, None)
            mapped_charging = map_charging_ocm(entry, None)
            mapped_station = map_station_ocm(entry, self.country_code)
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            station_updater.update_station(station=mapped_station, data_source_key='OCM')
        station_updater.log_update_station_counts()
