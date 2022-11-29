import configparser
import os
import pathlib
import pandas as pd

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from tqdm import tqdm

from mapping.charging import map_charging_fra
from mapping.stations import map_address_fra, map_station_fra
from services.excel_file_loader_service import ExcelFileLoaderService
from utils.bna_crawler import get_bna_data
from utils.logging_utils import log


class FraPipeline:
    def __init__(self, config: configparser, session: Session, offline: bool = False):
        self.config = config
        self.session = session
        self.offline: bool = offline

    def _retrieve_data(self):
        data_dir: str = os.path.join(
            pathlib.Path(__file__).parent.resolve(), "..", "data"
        )
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_data_path = os.path.join(data_dir, self.config["FRGOV"]["filename"])
        #if not self.offline:
            #get_bna_data(tmp_data_path)
        self.data = pd.read_csv(os.path.join(data_dir, "france_stations.csv"), delimiter= ",", encoding= "utf-8", encoding_errors= "replace")

    def run(self):
        self._retrieve_data()
        self.data.drop_duplicates(subset=["id_station_itinerance"], inplace=True)
        for _, row in tqdm(self.data.iterrows()):
            mapped_address = map_address_fra(row, None)
            mapped_charging = map_charging_fra(row, None)
            mapped_station = map_station_fra(row)
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging

            self.session.add(mapped_station)
            try:
                self.session.commit()
                self.session.flush()
            except IntegrityError as e:
                log.error(f"FRA-Entry exists already! Error: {e}")
                print(e)
                self.session.rollback()
                continue
            except Exception as e:
                log.error(f"FRA-Pipeline failed to run! Error: {e}")
                print(e)
                self.session.rollback()
