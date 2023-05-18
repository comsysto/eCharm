import configparser
import logging
import os
import pathlib

import pandas as pd
import requests as requests
from bs4 import BeautifulSoup
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.mapping.charging import map_charging_fra
from charging_stations_pipelines.mapping.stations import map_address_fra, map_station_fra
from charging_stations_pipelines.shared import reject_if, download_file

logger = logging.getLogger(__name__)


class FraPipeline:
    def __init__(self, config: configparser, session: Session, online: bool = False):
        self.config = config
        self.session = session
        self.online: bool = online

    def _retrieve_data(self):
        data_dir: str = os.path.join(
            pathlib.Path(__file__).parent.resolve(), "../../..", "data"
        )
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_data_path = os.path.join(data_dir, self.config["FRGOV"]["filename"])
        if self.online:
            self.download_france_gov_file(tmp_data_path)
        self.data = pd.read_csv(os.path.join(data_dir, "france_stations.csv"), delimiter=",", encoding="utf-8",
                                encoding_errors="replace")

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
                logger.error(f"FRA-Entry exists already! Error: {e}")
                self.session.rollback()
                continue
            except Exception as e:
                logger.error(f"FRA-Pipeline failed to run! Error: {e}")
                self.session.rollback()

    @staticmethod
    def download_france_gov_file(target_file):
        base_url = "https://transport.data.gouv.fr/resources/79624"

        r = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.content, "html.parser")

        all_links_on_gov_page = soup.findAll('a')

        link_to_dataset = list(
            filter(lambda a: a["href"].startswith("https://www.data.gouv.fr/fr/datasets"), all_links_on_gov_page))
        reject_if(len(link_to_dataset) != 1, "Could not determine source for french government data")
        download_file(link_to_dataset[0]["href"], target_file)
