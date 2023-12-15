"""Pipeline for retrieving data from the French government website."""

import configparser
import logging
from pathlib import Path
from typing import Final

import pandas as pd
import requests as requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.pipelines import Pipeline
from charging_stations_pipelines.pipelines.fr.france_mapper import map_address_fra, map_charging_fra, map_station_fra
from charging_stations_pipelines.pipelines.station_table_updater import StationTableUpdater
from . import DATA_SOURCE_KEY
from ... import PROJ_DATA_DIR
from ...file_utils import download_file
from ...shared import reject_if

logger = logging.getLogger(__name__)


class FraPipeline(Pipeline):

    def __init__(self, config: configparser, session: Session, online=False):
        super().__init__(config, session, online)

        # All data is from France
        self.country_code: Final[str] = "FR"

        self.data_path: Final[Path] = PROJ_DATA_DIR / config[DATA_SOURCE_KEY]["filename"]

    def retrieve_data(self) -> None:
        if self.online:
            logger.info("Retrieving Online Data")
            self.download_france_gov_file(self.data_path)

        self.data = pd.read_csv(self.data_path, delimiter=",", encoding="utf-8", encoding_errors="replace")
        self.data.drop_duplicates(subset=["id_station_itinerance"], inplace=True)

    def run(self):
        logger.info(f"Running {DATA_SOURCE_KEY} Pipeline...")
        self.retrieve_data()

        station_updater = StationTableUpdater(session=self.session, logger=logger)
        row: pd.Series
        for _, row in tqdm(self.data.iterrows(), total=self.data.shape[0]):
            mapped_station = map_station_fra(row, self.country_code)
            mapped_station.address = map_address_fra(row, self.country_code)
            mapped_station.charging = map_charging_fra(row)
            station_updater.update_station(mapped_station, DATA_SOURCE_KEY)
        station_updater.log_update_station_counts()

    @staticmethod
    def download_france_gov_file(target_file: Path) -> None:
        """Download a file from the French government website."""
        base_url = "https://transport.data.gouv.fr/resources/79624"

        r = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.content, "html.parser")

        all_links_on_gov_page = soup.findAll('a')

        link_to_dataset = list(
                filter(lambda a: a["href"].startswith("https://www.data.gouv.fr/fr/datasets"), all_links_on_gov_page))
        reject_if(len(link_to_dataset) != 1, f"Could not determine source for {DATA_SOURCE_KEY} data")

        download_file(link_to_dataset[0]["href"], target_file)
