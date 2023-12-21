"""This module contains the OSM Pipeline."""
import collections
import configparser
import json
import logging
import os
import pathlib

from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.pipelines import Pipeline
from charging_stations_pipelines.pipelines.osm import (
    DATA_SOURCE_KEY,
    osm_mapper,
    SCOPE_COUNTRIES,
)
from charging_stations_pipelines.pipelines.osm.osm_receiver import get_osm_data
from charging_stations_pipelines.pipelines.station_table_updater import (
    StationTableUpdater,
)
from charging_stations_pipelines.shared import JSON

logger = logging.getLogger(__name__)


class OsmPipeline(Pipeline):
    """Pipeline for the OSM data source."""

    def __init__(
        self,
        country_code: str,
        config: configparser,
        session: Session,
        online: bool = False,
    ):
        super().__init__(config, session, online)

        self.country_code = country_code

    def retrieve_data(self):
        data_dir: str = os.path.join(
            pathlib.Path(__file__).parent.resolve(), "../../..", "data"
        )
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_file_path = os.path.join(data_dir, self.config[DATA_SOURCE_KEY]["filename"])
        if self.online:
            logger.info("Retrieving Online Data")
            get_osm_data(self.country_code, tmp_file_path)
        with open(tmp_file_path) as f:
            self.data = json.load(f)

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
                if station.country_code not in SCOPE_COUNTRIES:
                    stats["count_country_mismatch_stations"] += 1

                # Address mapping
                station.address = osm_mapper.map_address_osm(entry, None)

                # Count stations which have an invalid address
                if (
                    station.address
                    and station.address.country
                    and station.address.country not in SCOPE_COUNTRIES
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
                logger.debug(f"{DATA_SOURCE_KEY} entry could not be parsed, error: {ex}. Row: {entry}")

        logger.info(
            f"Finished {DATA_SOURCE_KEY} Pipeline:\n"
            f"1. Valid stations found: {stats['count_valid_stations']}\n"
            f"2. Not parseable stations: {stats['count_parse_error']}\n"
            f"3. Wrong country code stations: {stats['count_country_mismatch_stations']}."
        )

        station_updater.log_update_station_counts()
