"""This module handles the mapping of charging data obtained from e-control.at (also known as ladestellen.at).
The data is structured into three main objects:
- 'Station': Represents the charging station.
- 'Address': Contains address information for the charging station.
- 'Charging': Provides the charging details for a particular charging station.
"""
import collections
import configparser
import logging

import pandas as pd
from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.pipelines import Pipeline
from charging_stations_pipelines.pipelines.at import DATA_SOURCE_KEY
from charging_stations_pipelines.pipelines.at.econtrol_crawler import get_data
from charging_stations_pipelines.pipelines.at.econtrol_mapper import (
    map_address,
    map_charging,
    map_station,
)
from charging_stations_pipelines.pipelines.station_table_updater import (
    StationTableUpdater,
)
from charging_stations_pipelines.shared import country_import_data_path

logger = logging.getLogger(__name__)


class EcontrolAtPipeline(Pipeline):
    """:class:`EcontrolAtPipeline` is a class that represents a pipeline for processing data
    from the e-control.at (aka ladestellen.at) - an official data source from the Austrian government.

    :param config: A `configparser` object containing configurations for the pipeline.
    :param session: A `Session` object representing the session used for database operations.
    :param online: A boolean indicating whether the pipeline should retrieve data online. Default is False.

    :ivar config: A `configparser` object containing configurations for the pipeline.
    :ivar session: A `Session` object representing the session used for database operations.
    :ivar online: A boolean indicating whether the pipeline should retrieve data online.
    """

    def __init__(self, config: configparser, session: Session, online: bool = False):
        super().__init__(config, session, online)

        # Is always 'AT' for this pipeline
        self.country_code = "AT"

    def _retrieve_data(self):
        tmp_data_path = country_import_data_path(self.country_code) / self.config[DATA_SOURCE_KEY]["filename"]
        if self.online:
            logger.info("Retrieving Online Data")
            get_data(tmp_data_path)
        # NOTE, read data from json file in NDJSON (newline delimited JSON) format,
        #   i.e. one json object per line, thus `lines=True` is required
        self.data = pd.read_json(tmp_data_path, lines=True)  # pd.DataFrame

    def run(self):
        """Runs the pipeline for a data source.
        Retrieves data, processes it, and updates the Station table (as well as Address and Charging tables).

        :return: None
        """
        logger.info(f"Running {DATA_SOURCE_KEY} Pipeline...")
        self._retrieve_data()
        station_updater = StationTableUpdater(session=self.session, logger=logger)

        stats = collections.defaultdict(int)
        datapoint: pd.Series
        for _, datapoint in tqdm(iterable=self.data.iterrows(), total=self.data.shape[0]):
            try:
                station = map_station(datapoint, self.country_code)

                # Address mapping
                station.address = map_address(datapoint, self.country_code, None)

                # Count stations that have some kind of country code mismatch
                if (
                    # Count stations which have an invalid country code in address
                    (station.address and station.address.country and station.address.country != "AT")
                    # Count stations which have a mismatching country code between Station and Address
                    or (
                        station.country_code is not None
                        and station.address is not None
                        and station.address.country is not None
                        and station.country_code != station.address.country
                    )
                ):
                    stats["count_country_mismatch_stations"] += 1

                # Charging point
                station.charging = map_charging(datapoint, None)

                station_updater.update_station(station, DATA_SOURCE_KEY)
                stats["count_valid_stations"] += 1
            except Exception as e:
                stats["count_parse_error"] += 1
                logger.debug(
                    f"{DATA_SOURCE_KEY} entry could not be parsed, error:\n{e}\n" f"Row:\n----\n{datapoint}\n----\n"
                )
        logger.info(
            f"Finished {DATA_SOURCE_KEY} Pipeline:\n"
            f"1. Valid stations found: {stats['count_valid_stations']}\n"
            f"2. Not parseable stations: {stats['count_parse_error']}\n"
            f"3. Wrong country code stations: {stats['count_country_mismatch_stations']}."
        )
        station_updater.log_update_station_counts()
