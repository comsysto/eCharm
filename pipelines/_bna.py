import configparser
import os
import pathlib

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from tqdm import tqdm

from mapping.charging import map_charging_bna
from mapping.stations import map_address_bna, map_station_bna
from services.excel_file_loader_service import ExcelFileLoaderService
from utils.bna_crawler import get_bna_data
from utils.logging_utils import log


class BnaPipeline:
    def __init__(self, config: configparser, session: Session, offline: bool = False):
        self.config = config
        self.session = session
        self.offline: bool = offline

    def _retrieve_data(self):
        data_dir: str = os.path.join(
            pathlib.Path(__file__).parent.resolve(), "..", "data"
        )
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_data_path = os.path.join(data_dir, self.config["BNA"]["filename"])
        if not self.offline:
            get_bna_data(tmp_data_path)
        self.data = ExcelFileLoaderService().load(tmp_data_path)

    def run(self):
        self._retrieve_data()
        for _, row in tqdm(self.data.iterrows()):
            mapped_address = map_address_bna(row, None)
            mapped_charging = map_charging_bna(row, None)
            mapped_station = map_station_bna(row)
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            self.session.add(mapped_station)
            try:
                self.session.commit()
                self.session.flush()
            except IntegrityError as e:
                log.debug(f"BNA-Entry exists already! Error: {e}")
            except Exception as e:
                log.error(f"BNA-Pipeline failed to run! Error: {e}")
                self.session.rollback()
