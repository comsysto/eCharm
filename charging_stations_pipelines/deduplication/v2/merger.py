import logging

from geoalchemy2 import func
from geoalchemy2.shape import to_shape, from_shape
from shapely.speedups._speedups import Point
from sqlalchemy.orm import joinedload

from charging_stations_pipelines.deduplication.v2.multiprocess_duplicate_finder import MultiProcessDuplicateFinder
from charging_stations_pipelines.deduplication.v2.types import Station, StationDuplicate
from charging_stations_pipelines.models.station import Station as StationEntity

logger = logging.getLogger(__name__)


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
        address=street + " " + town if street and town else None,
    )


class ChargingStationMerger:
    def __init__(self, country_code, db_session):
        self.country_code = country_code
        self.db_session = db_session
        self.multi_process_duplicate_finder = MultiProcessDuplicateFinder()

    def run(self):
        munich = from_shape(Point(float(11.576124), float(48.137154)))
        all_stations_in_db: list[StationEntity] = (self.db_session.query(StationEntity)
                                                   .filter(StationEntity.country_code == self.country_code)
                                                   .filter(func.ST_Distance(StationEntity.point, munich) < 10000)
                                                   .options(joinedload(StationEntity.address))
                                                   .all())
        self.db_session.expunge_all()
        self.db_session.close()

        all_stations: list[Station] = [_to_station(station) for station in all_stations_in_db]

        merged_stations: list[Station] = self.merge(all_stations)

        logger.info(f"Total: {len(all_stations_in_db)}\t merged: {len(merged_stations)}")

    def merge(self, all_stations: list[Station]) -> list[Station]:

        logger.info(f"Checking {len(all_stations)} stations for duplicates")

        station_with_duplicates: list[StationDuplicate] = self.multi_process_duplicate_finder.run(all_stations)

        logger.info(f"Found {len(station_with_duplicates)} duplicated pairs")

        # find duplicates for every station of all_stations
        # for station in all_stations:
        #
        #     duplicates, ratios = self.find_duplicates(station, all_stations)
        #
        #     if len(duplicates) == 0:
        #         all_stations.remove(station)
        #
        #     for i in range(len(duplicates)):
        #         station_with_duplicates.append(StationDuplicate(station, duplicates[i], ratios[i]))

        # merge duplicates into station
        # merged_station = self.merge_duplicates(station, duplicates)

        # add merged_station to merged_stations
        # merged_stations.append(merged_station)

        if len(station_with_duplicates) > 0:
            self.export_duplicates(station_with_duplicates)

        return []

    @staticmethod
    def merge_duplicates(station, duplicates):
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
                    f"{duplicate.ratio}\n")
