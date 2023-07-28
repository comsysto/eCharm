import configparser
import json
import logging
import os
import pathlib
from typing import Dict, Optional

from sqlalchemy.orm import Session

from charging_stations_pipelines.pipelines.gb.gb_mapper import map_address_gb, map_charging_gb, map_station_gb
from charging_stations_pipelines.pipelines.gb.gb_receiver import get_gb_data
from charging_stations_pipelines.pipelines.station_table_updater import StationTableUpdater

logger = logging.getLogger(__name__)


class GbPipeline:
    def __init__(self, config: configparser, session: Session, onlinw: bool = False):
        self.config = config
        self.session = session
        self.online: bool = onlinw
        self.data: Optional[Dict] = None

    def _retrieve_data(self):
        data_dir: str = os.path.join(
            pathlib.Path(__file__).parent.resolve(), "../../..", "data"
        )
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_file_path = os.path.join(data_dir, self.config["GBGOV"]["filename"])
        if self.online:
            logger.info("Retrieving Online Data")
            get_gb_data(tmp_file_path)
        with open(tmp_file_path, "r") as f:
            self.data = json.load(f)

    def run(self):

        logger.info("Running GB GOV Pipeline...")

        self._retrieve_data()

        station_updater = StationTableUpdater(session=self.session, logger=logger)
        entry: Dict
        for entry in self.data.get("ChargeDevice", []):
            mapped_address = map_address_gb(entry, None)
            mapped_charging = map_charging_gb(entry)
            mapped_station = map_station_gb(entry, "GB")
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            station_updater.update_station(station=mapped_station, data_source_key='GBGOV')
        station_updater.log_update_station_counts()
