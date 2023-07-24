import logging
from dataclasses import dataclass
from typing import Tuple

from geoalchemy2.shape import to_shape
from sqlalchemy.orm import joinedload

from charging_stations_pipelines.deduplication.v2.ratio_calculator import RatioCalculator
from charging_stations_pipelines.deduplication.v2.types import Station
from charging_stations_pipelines.models.station import Station as StationEntity

logger = logging.getLogger(__name__)

THRESHOLD_OVERALL = 0.9


@dataclass(frozen=True)
class StationDuplicate:
    station: Station
    duplicate: Station
    delta: float


def _to_station(station_entity: StationEntity):
    street = None
    town = None
    if station_entity.address:
        street = station_entity.address.street.lower() if station_entity.address.street else None
        town = station_entity.address.town.lower() if station_entity.address.town else None

    return Station(
        identifier=station_entity.id,
        data_source=station_entity.data_source,
        operator=station_entity.operator.lower() if station_entity.operator else None,
        point=to_shape(station_entity.point),
        address= street + " " + town if street and town else None,
    )


class ChargingStationMerger:
    def __init__(self, country_code, db_session):
        self.country_code = country_code
        self.db_session = db_session
        self.delta_calculator = RatioCalculator()

    def run(self):
        # load all all_stations_in_db from the database
        all_stations_in_db: list[StationEntity] = self.db_session.query(StationEntity).filter(
            StationEntity.country_code == self.country_code).options(
            joinedload(StationEntity.address)
        ).all()

        # detach all all_stations_in_db from the session
        self.db_session.expunge_all()

        # map all_stations_in_db to Station objects
        all_stations: list[Station] = [_to_station(station) for station in all_stations_in_db]

        # merge all_stations
        merged_stations: list[Station] = self.merge(all_stations)

        logger.info(f"Total: {len(all_stations_in_db)}\t merged: {len(merged_stations)}")

    def merge(self, all_stations: list[Station]) -> list[Station]:

        station_with_duplicates: list[StationDuplicate] = []

        count = 0

        # find duplicates for every station of all_stations
        for station in all_stations:
            if count % 10 == 0:
                logger.info(f"Processing station {count} of {len(all_stations)}")

            if count > 100:
                break

            duplicates, ratios = self.find_duplicates(station, all_stations)

            # remove station from all_stations if it has no duplicates
            if len(duplicates) == 0:
                all_stations.remove(station)

            for i in range(len(duplicates)):
                station_with_duplicates.append(StationDuplicate(station, duplicates[i], ratios[i]))

            # merge duplicates into station
            # merged_station = self.merge_duplicates(station, duplicates)

            # add merged_station to merged_stations
            # merged_stations.append(merged_station)

            count += 1

        # export duplicates to csv
        if len(station_with_duplicates) > 0:
            self.export_duplicates(station_with_duplicates)

        return []

    def find_duplicates(self, station, all_stations: list[Station]) -> Tuple[list[Station], list[float]]:
        duplicates: list[Station] = []
        ratios: list[float] = []
        for s in all_stations:

            if station == s:
                continue

            ratio = self.delta_calculator.ratio(station, s)
            if ratio > THRESHOLD_OVERALL:
                duplicates.append(s)
                ratios.append(ratio)

        return duplicates, ratios

    def merge_duplicates(self, station, duplicates):
        return station

    @staticmethod
    def export_duplicates(duplicates):
        # write duplicates to csv file
        with open("duplicates.csv", "w") as f:
            f.write("station_id,source_lng,source_lat,station_operator,station_address,"
                    "duplicate_id,target_lng,target_lat,duplicate_operator,duplicate_address,"
                    "delta\n")
            for (duplicate) in duplicates:
                f.write(
                    f"{duplicate.station.identifier},{duplicate.station.point.x},{duplicate.station.point.y},{duplicate.station.operator},{duplicate.station.address},"
                    f"{duplicate.duplicate.identifier},{duplicate.duplicate.point.x},{duplicate.duplicate.point.y},{duplicate.duplicate.operator},{duplicate.duplicate.address},"
                    f"{duplicate.delta}\n")
