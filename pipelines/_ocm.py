import configparser
import json
import os
import pathlib
from typing import Dict, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from mapping.charging import map_charging_ocm
from mapping.stations import map_address_ocm, map_station_ocm
from utils.logging_utils import log
from utils.ocm_extractor import ocm_extractor


class OcmPipeline:
    def __init__(self, config: configparser, session: Session, offline: bool = False):
        self.config = config
        self.session = session
        self.offline: bool = offline
        self.data: Optional[Dict] = None

    def _retrieve_data(self):
        data_dir: str = os.path.join(
            pathlib.Path(__file__).parent.resolve(), "..", "data"
        )
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_file_path = os.path.join(data_dir, self.config["OCM"]["filename"])
        if not self.offline:
            ocm_extractor(tmp_file_path)
        with open(tmp_file_path, "r") as f:
            self.data = json.load(f)

    def run(self):
        self._retrieve_data()
        entry: Dict
        for id, entry in self.data.items():
            mapped_address = map_address_ocm(entry, None)
            mapped_charging = map_charging_ocm(entry, None)
            mapped_station = map_station_ocm(entry)
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            self.session.add(mapped_station)
            try:
                self.session.commit()
                self.session.flush()
            except IntegrityError as e:
                log.debug(f"OSM-Entry exists already! Error: {e}")
            except Exception as e:
                log.error(f"OSM-Pipeline failed to run! Error: {e}")
                self.session.rollback()
