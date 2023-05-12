import configparser
import json
import logging
import os
import pathlib
from typing import Dict, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from charging_stations_pipelines.mapping.charging import map_charging_gb
from charging_stations_pipelines.mapping.stations import map_address_gb, map_station_gb
from charging_stations_pipelines.pipelines.gb.gb_receiver import get_gb_data

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
            get_gb_data(tmp_file_path)
        with open(tmp_file_path, "r") as f:
            self.data = json.load(f)

    def run(self):

        logger.debug("Running GB Pipeline...")

        self._retrieve_data()
        entry: Dict
        for entry in self.data.get("ChargeDevice", []):
            mapped_address = map_address_gb(entry, None)
            mapped_charging = map_charging_gb(entry, None)
            mapped_station = map_station_gb(entry, "GB")
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            self.session.add(mapped_station)
            try:
                self.session.commit()
                self.session.flush()
            except IntegrityError as e:
                logger.error(f"GBGOV-Entry exists already! Error: {e}")
                self.session.rollback()
                continue
            except Exception as e:
                logger.error(f"GB-Pipeline failed to run! Error: {e}")
                self.session.rollback()
