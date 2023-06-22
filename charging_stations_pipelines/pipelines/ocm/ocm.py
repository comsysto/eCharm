import configparser
import json
import logging
import os
import pathlib
from typing import Dict, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from charging_stations_pipelines.pipelines.ocm.ocm_extractor import ocm_extractor
from charging_stations_pipelines.pipelines.ocm.ocm_mapper import map_address_ocm, map_station_ocm, map_charging_ocm

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
            ocm_extractor(tmp_file_path, self.country_code)
        with open(tmp_file_path, "r") as f:
            self.data = json.load(f)

    def run(self):
        self._retrieve_data()
        entry: Dict
        for id, entry in self.data.items():
            mapped_address = map_address_ocm(entry, None)
            mapped_charging = map_charging_ocm(entry, None)
            mapped_station = map_station_ocm(entry, self.country_code)
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            self.session.add(mapped_station)
            try:
                self.session.commit()
                self.session.flush()
            except IntegrityError as e:
                logger.error(f"OCM-Entry exists already! Error: {e}")
                self.session.rollback()
                continue
            except Exception as e:
                logger.error(f"OCM-Pipeline failed to run! Error: {e}")
                self.session.rollback()
