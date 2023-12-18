"""This module contains the OSM Pipeline."""

import collections
import configparser
import json
import logging
from pathlib import Path
from typing import Final

from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.file_utils import append_country_code_to_file_name
from charging_stations_pipelines.pipelines import Pipeline
from charging_stations_pipelines.pipelines.osm import (
    DATA_SOURCE_KEY,
    OSM_DATA_PATH, osm_mapper,
    SUPPORTED_COUNTRIES,
)
from charging_stations_pipelines.pipelines.osm.osm_receiver import get_osm_data
from charging_stations_pipelines.pipelines.station_table_updater import StationTableUpdater
from charging_stations_pipelines.shared import JSON

logger = logging.getLogger(__name__)


class OsmPipeline(Pipeline):

    def __init__(self, country_code: str, config: configparser, session: Session, online=False):
        super().__init__(config, session, online)

        self.country_code: Final[str] =  country_code.upper()

        self.data_path: Final[Path] = OSM_DATA_PATH / append_country_code_to_file_name(
                config[DATA_SOURCE_KEY]["filename"], self.country_code)
        self.data_path.parent.mkdir(parents=True, exist_ok=True)

    def retrieve_data(self):
        if self.online:
            logger.info(f"Retrieving {DATA_SOURCE_KEY}/{self.country_code} Online Data...")
            get_osm_data(self.country_code, self.data_path)

        with self.data_path.open() as file:
            self.data = json.load(file)
            logger.info(f"Finished retrieving data to file: {self.data_path}")

    def run(self):
        logger.info(f"Running {self.country_code} {DATA_SOURCE_KEY} Pipeline...")
        self.retrieve_data()

        station_updater = StationTableUpdater(session=self.session, logger=logger)
        stats = collections.defaultdict(int)
        data_elements: JSON = self.data.get("elements", [])
        entry: JSON
        for entry in tqdm(iterable=iter(data_elements), total=len(data_elements)):
            try:
                station = osm_mapper.map_station_osm(entry, self.country_code)

                # Count stations with country codes that are not in the scope of the pipeline
                if station.country_code not in SUPPORTED_COUNTRIES:
                    stats["count_country_mismatch_stations"] += 1

                # Address mapping
                station.address = osm_mapper.map_address_osm(entry, None)

                # Count stations which have an invalid address
                if (
                    station.address
                    and station.address.country
                    and station.address.country not in SUPPORTED_COUNTRIES
                ):
                    stats["count_country_mismatch_stations"] += 1

                # Count stations which have a mismatching country code between Station and Address
                if (
                    station.country_code is not None
                    and station.address is not None
                    and station.address.country is not None
                    and station.country_code != station.address.country
                ):
                    stats["count_country_mismatch_stations"] += 1

                # Charging point
                station.charging = osm_mapper.map_charging_osm(entry, None)

                station_updater.update_station(station, DATA_SOURCE_KEY)
                stats["count_valid_stations"] += 1
            except Exception as ex:
                stats["count_parse_error"] += 1
                logger.warning(f"{DATA_SOURCE_KEY} entry could not be parsed, error: {ex}. Row: {entry}")
        logger.warning(
            f"Finished {DATA_SOURCE_KEY} Pipeline:\n"
            f"1. Valid stations found: {stats['count_valid_stations']}\n"
            f"2. Not parseable stations: {stats['count_parse_error']}\n"
            f"3. Wrong country code stations: {stats['count_country_mismatch_stations']}."
        )
        station_updater.log_update_station_counts()
