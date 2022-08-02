import configparser
import os
import pathlib

from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

from db import Session
from mapping.charging import map_charging_bna
from mapping.stations import map_address_bna, map_stations_bna
from services.excel_file_loader_service import ExcelFileLoaderService
from utils.bna_crawler import get_bna_data
from utils.logging_utils import log


class BnaPipeline:
    def __init__(
        self, config: configparser, db_session: Session, offline: bool = False
    ):
        self.config = config
        self.db_session = db_session
        self.offline: bool = offline

    def _retrieve_data(self, config):
        tmp_data_path = os.path.join(
            pathlib.Path(__file__).parent.resolve(), "..", config["EXCEL"]["path"]
        )
        if not self.offline:
            get_bna_data(tmp_data_path)
        self.excel_df = ExcelFileLoaderService().load(tmp_data_path)

    def run(self):
        self._retrieve_data(self.config)
        try:
            for _, row in tqdm(self.excel_df.iterrows()):
                mapped_station = map_stations_bna(row)
                mapped_address = map_address_bna(row, mapped_station.id)
                mapped_charging = map_charging_bna(row, mapped_station.id)
                self.db_session.bulk_save_objects(
                    [mapped_station, mapped_address, mapped_charging]
                )
                self.db_session.commit()
        except IntegrityError as e:
            log.debug("BNA-Entry exists already!")
        except Exception as e:
            log.error("BNA pipeline failed to run.", e)
            self.db_session.rollback()
