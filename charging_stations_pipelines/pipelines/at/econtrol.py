"""This module handles the mapping of charging data obtained from e-control.at (also known as ladestellen.at).
The data is structured into three main objects:
- 'Station': Represents the charging station.
- 'Address': Contains address information for the charging station.
- 'Charging': Provides the charging details for a particular charging station.
"""

import collections
import configparser
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines import PROJ_DATA_DIR
from charging_stations_pipelines.pipelines import Pipeline
from charging_stations_pipelines.pipelines.at.econtrol_crawler import get_data
from charging_stations_pipelines.pipelines.at.econtrol_mapper import (
    map_address,
    map_charging,
    map_station,
)
from charging_stations_pipelines.pipelines.station_table_updater import StationTableUpdater
from . import DATA_SOURCE_KEY, SUPPORTED_COUNTRIES

logger = logging.getLogger(__name__)


class EcontrolAtPipeline(Pipeline):
    """Class that represents a pipeline for processing data from the e-control.at (aka ladestellen.at)
    - an official data source from the Austrian government.
    """

    def __init__(self, config: configparser, session: Session, online=False):
        super().__init__(config, session, online)

        # Is always 'AT' for this pipeline
        self.country_code = "AT"

        self.data_path: Path = PROJ_DATA_DIR / self.config[DATA_SOURCE_KEY]["filename"]
        self.data_path.parent.mkdir(parents=True, exist_ok=True)

    def retrieve_data(self) -> None:
        if self.online:
            logger.debug("Retrieving online data")
            get_data(self.data_path)
        # NOTE, read data from json file in NDJSON (newline delimited JSON) format,
        #   i.e. one json object per line, thus `lines=True` is required
        self.data = pd.read_json(self.data_path, lines=True)  # pd.DataFrame

    def run(self) -> None:
        logger.info(f"Running {DATA_SOURCE_KEY} Pipeline...")
        self.retrieve_data()

        station_updater = StationTableUpdater(self.session, logger)
        stats = collections.defaultdict(int)
        datapoint: pd.Series
        for _, datapoint in tqdm(iterable=self.data.iterrows(), total=self.data.shape[0]):
            try:
                station = map_station(datapoint, self.country_code)

                # Count stations with country codes that are not in the scope of the pipeline
                if station.country_code not in SUPPORTED_COUNTRIES:
                    stats["count_country_mismatch_stations"] += 1

                # Address mapping
                station.address = map_address(datapoint, self.country_code, None)

                # Count stations which have an invalid address
                if (
                    station.address
                    and station.address.country
                    and station.address.country not in SUPPORTED_COUNTRIES
                ):
                    stats["count_country_mismatch_stations"] += 1

                # Count stations which have a mismatching country code between Station and Address
                if station.country_code != station.address.country:
                    stats["count_country_mismatch_stations"] += 1

                # Charging point
                station.charging = map_charging(datapoint, None)

                station_updater.update_station(station, DATA_SOURCE_KEY)
                stats["count_valid_stations"] += 1
            except Exception as e:
                stats["count_parse_error"] += 1
                logger.warning(
                    f"{DATA_SOURCE_KEY} entry could not be parsed, error:\n{e}\n"
                    f"Row:\n----\n{datapoint}\n----\n"
                )
        logger.debug(
            f"Finished {DATA_SOURCE_KEY} Pipeline:\n"
            f"1. Valid stations found: {stats['count_valid_stations']}\n"
            f"2. Not parseable stations: {stats['count_parse_error']}\n"
            f"3. Wrong country code stations: {stats['count_country_mismatch_stations']}."
        )
        station_updater.log_update_station_counts()
