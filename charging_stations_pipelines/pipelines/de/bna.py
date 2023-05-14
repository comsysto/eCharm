import configparser
import logging
import os
import pathlib

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.mapping.charging import map_charging_bna
from charging_stations_pipelines.mapping.stations import map_address_bna, map_station_bna
from charging_stations_pipelines.pipelines.de.bna_crawler import get_bna_data
from charging_stations_pipelines.shared import load_excel_file

logger = logging.getLogger(__name__)


class BnaPipeline:
    def __init__(self, config: configparser, session: Session, online: bool = False):
        self.config = config
        self.session = session
        self.online: bool = online
        relative_dir = os.path.join("../../..", "data")
        self.data_dir: str = os.path.join(pathlib.Path(__file__).parent.resolve(), relative_dir)

    def _retrieve_data(self):
        pathlib.Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        tmp_data_path = os.path.join(self.data_dir, self.config["BNA"]["filename"])
        if self.online:
            get_bna_data(tmp_data_path)
        self.data = load_excel_file(tmp_data_path)

    def run(self):
        self._retrieve_data()
        for _, row in tqdm(self.data.iterrows()):
            try:
                mapped_address = map_address_bna(row, None)
                mapped_charging = map_charging_bna(row, None)
                mapped_station = map_station_bna(row)
                mapped_station.address = mapped_address
                mapped_station.charging = mapped_charging
                self.session.add(mapped_station)
            except Exception as e:
                logger.error(f"BNA-Entry could not be mapped! Error: {e}")
            try:
                self.session.commit()
                self.session.flush()
            except IntegrityError as e:
                logger.error(f"BNA-Entry exists already! Error: {e}")
                self.session.rollback()
                continue
            except Exception as e:
                logger.error(f"BNA-Pipeline failed to run! Error: {e}")
                self.session.rollback()
