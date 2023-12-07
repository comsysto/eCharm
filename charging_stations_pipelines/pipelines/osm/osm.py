import configparser
import json
import logging
import os
import pathlib
from typing import Dict, Optional

from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.pipelines.osm import DATA_SOURCE_KEY
from charging_stations_pipelines.pipelines.osm.osm_mapper import map_address_osm, map_charging_osm, map_station_osm
from charging_stations_pipelines.pipelines.osm.osm_receiver import get_osm_data
from charging_stations_pipelines.pipelines.station_table_updater import StationTableUpdater

logger = logging.getLogger(__name__)


class OsmPipeline:
    def __init__(self, country_code: str, config: configparser, session: Session, online: bool = False):
        self.country_code = country_code
        self.config = config
        self.session = session
        self.online: bool = online
        self.data: Optional[Dict] = None

    def _retrieve_data(self):
        data_dir: str = os.path.join(
            pathlib.Path(__file__).parent.resolve(), "../../..", "data"
        )
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_file_path = os.path.join(data_dir, self.config[DATA_SOURCE_KEY]["filename"])
        if self.online:
            logger.info("Retrieving Online Data")
            get_osm_data(self.country_code, tmp_file_path)
        with open(tmp_file_path) as f:
            self.data = json.load(f)

    def run(self):
        logger.info(f"Running {self.country_code} {DATA_SOURCE_KEY} Pipeline...")
        self._retrieve_data()
        station_updater = StationTableUpdater(session=self.session, logger=logger)
        data = self.data.get("elements", [])
        for entry in tqdm(iterable=iter(data), total=len(data)):  # type: Dict
            mapped_address = map_address_osm(entry, None)
            mapped_charging = map_charging_osm(entry, None)
            mapped_station = map_station_osm(entry, self.country_code)
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            station_updater.update_station(mapped_station, DATA_SOURCE_KEY)
        station_updater.log_update_station_counts()
