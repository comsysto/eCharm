import configparser
import json
import logging
import os
import pathlib
from typing import Dict, Optional

from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.pipelines.ocm.ocm_extractor import ocm_extractor
from charging_stations_pipelines.pipelines.ocm.ocm_mapper import map_address_ocm, map_station_ocm, map_charging_ocm
from charging_stations_pipelines.pipelines.station_table_updater import StationTableUpdater

logger = logging.getLogger(__name__)


class OcmPipeline:
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
        tmp_file_path = os.path.join(data_dir, self.config["OCM"]["filename"])
        if self.online:
            logger.info("Retrieving Online Data")
            ocm_extractor(tmp_file_path, self.country_code)
        with open(tmp_file_path, "r") as f:
            self.data = json.load(f)

    def run(self):
        logger.info(f"Running {self.country_code} OCM Pipeline...")
        self._retrieve_data()
        station_updater = StationTableUpdater(session=self.session, logger=logger)
        entry: Dict
        data = self.data.items()
        for id, entry in tqdm(iterable=iter(data), total=len(data)):
            mapped_address = map_address_ocm(entry, None)
            mapped_charging = map_charging_ocm(entry, None)
            mapped_station = map_station_ocm(entry, self.country_code)
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            station_updater.update_station(station=mapped_station, data_source_key='OCM')
        station_updater.log_update_station_counts()
