import configparser
import json
import os
import pathlib
from typing import Dict, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from mapping.charging import map_charging_gb
from mapping.stations import map_address_gb, map_station_gb
from services.excel_file_loader_service import ExcelFileLoaderService
from utils.gb_receiver import get_gb_data
from utils.logging_utils import log


class GbPipeline:
    def __init__(self, config: configparser, session: Session, offline: bool = False):
        self.config = config
        self.session = session
        self.offline: bool = offline

    def __init__(self, country_code:str, config: configparser, session: Session, offline: bool = False):
        self.country_code = country_code
        self.config = config
        self.session = session
        self.offline: bool = offline
        self.data: Optional[Dict] = None

    def _retrieve_data(self):
        data_dir: str = os.path.join(
            pathlib.Path(__file__).parent.resolve(), "..", "data"
        )
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_file_path = os.path.join(data_dir, self.config["GBGOV"]["filename"])
        if not self.offline:
            get_gb_data(tmp_file_path, self.country_code)
        with open(tmp_file_path, "r") as f:
            self.data = json.load(f)

    def run(self):
        self._retrieve_data()
        entry: Dict
        for entry in self.data.get("ChargeDevice", []):
            mapped_address = map_address_gb(entry, None)
            mapped_charging = map_charging_gb(entry, None)
            mapped_station = map_station_gb(entry, self.country_code)
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            self.session.add(mapped_station)
            try:
                self.session.commit()
                self.session.flush()
            except IntegrityError as e:
                log.error(f"GBGOV-Entry exists already! Error: {e}")
                self.session.rollback()
                continue
            except Exception as e:
                log.error(f"GB-Pipeline failed to run! Error: {e}")
                self.session.rollback()

