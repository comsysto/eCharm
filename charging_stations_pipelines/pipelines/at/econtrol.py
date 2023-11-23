import configparser
import logging
import os
import pathlib

import pandas as pd
from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.pipelines.at import DATA_SOURCE_KEY
from charging_stations_pipelines.pipelines.at.econtrol_crawler import get_data
from charging_stations_pipelines.pipelines.at.econtrol_mapper import map_address, map_charging, map_station
from charging_stations_pipelines.pipelines.station_table_updater import StationTableUpdater

logger = logging.getLogger(__name__)


class EcontrolAtPipeline:
    def __init__(self, config: configparser, session: Session, online: bool = False):
        self.config = config
        self.session = session
        self.online: bool = online
        relative_dir = os.path.join("../../..", "data")
        self.data_dir: str = os.path.join(pathlib.Path(__file__).parent.resolve(), relative_dir)

    def _retrieve_data(self):
        pathlib.Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        tmp_data_path = os.path.join(self.data_dir, self.config[DATA_SOURCE_KEY]["filename"])
        if self.online:
            logger.info("Retrieving Online Data")
            get_data(tmp_data_path)
        # NOTE, read data from json file in NDJSON (newline delimited JSON) format,
        #   i.e. one json object per line, thus `lines=True` is required
        self.data = pd.read_json(tmp_data_path, lines=True)  # pd.DataFrame

    def run(self):
        logger.info(f"Running {DATA_SOURCE_KEY} Pipeline...")
        self._retrieve_data()
        station_updater = StationTableUpdater(session=self.session, logger=logger)
        for _, row in tqdm(iterable=self.data.iterrows(), total=self.data.shape[0]):
            try:
                station = map_station(row)
                station.address = map_address(row, None)
                station.charging = map_charging(row, None)
                # TODO check station, address, charging for validity,
                #  e.g. if
                #  - station.source_id is None
                #  - station.point is None
                #  skip such stations from saving to db
            except Exception as e:
                logger.error(f"{DATA_SOURCE_KEY} entry could not be mapped! Error: {e}")
                continue
            station_updater.update_station(station, DATA_SOURCE_KEY)
        station_updater.log_update_station_counts()
