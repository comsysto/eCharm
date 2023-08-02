import logging
from concurrent.futures import ThreadPoolExecutor

from geoalchemy2.shape import to_shape
from sqlalchemy.orm import joinedload

from charging_stations_pipelines.deduplication.v2.ratio_calculator import RatioCalculator
from charging_stations_pipelines.deduplication.v2.types import Station, StationDuplicate
from charging_stations_pipelines.models.station import Station as StationEntity

logger = logging.getLogger(__name__)

THRESHOLD_OVERALL = 0.9


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
        self.delta_calculator = RatioCalculator()

    def run(self):
        all_stations_in_db: list[StationEntity] = self.db_session.query(StationEntity).filter(
            StationEntity.country_code == self.country_code).options(
            joinedload(StationEntity.address)
        ).all()

        self.db_session.expunge_all()

        all_stations: list[Station] = [_to_station(station) for station in all_stations_in_db]

        merged_stations: list[Station] = self.merge(all_stations)

        logger.info(f"Total: {len(all_stations_in_db)}\t merged: {len(merged_stations)}")

    def merge(self, all_stations: list[Station]) -> list[Station]:

        station_with_duplicates: list[StationDuplicate] = []

        count = 0

        # use multiprocessing
        num_threads = 8

        # Divide the objects into subsets for each thread
        stations_per_thread = len(all_stations) // num_threads
        subsets = [all_stations[i:i+stations_per_thread] for i in range(0, len(all_stations), stations_per_thread)]

        # Step 3 & 4: Create ThreadPoolExecutor and submit tasks
        with ThreadPoolExecutor(max_workers=num_threads) as executor:

            futures = []

            for i, subset in enumerate(subsets):
                futures.append(executor.submit(self.find_duplicates, i, subset, all_stations.copy()))

            # Step 5: Wait for tasks to complete and merge results
            for future in futures:
                station_with_duplicates.extend(future.result())

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

    def find_duplicates(self, thread_id: int, subset: list[Station], all_stations: list[Station]):

        logger.info(f"Thread {thread_id} started with {len(subset)} stations")
        count = 0

        station_with_duplicates: list[StationDuplicate] = []

        for station in subset:
            count += 1

            logger.info(f"Thread {thread_id}: Processing station {count}")

            for s in all_stations:

                if station == s:
                    continue

                ratio = self.delta_calculator.ratio(station, s)
                logger.debug(f"ratio: {ratio}")
                logger.debug(f"station: {station}")
                logger.debug(f"s: {s}")

                if ratio > THRESHOLD_OVERALL:
                    station_with_duplicates.append(StationDuplicate(station, s, ratio))

        return station_with_duplicates

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
                    f"{duplicate.delta}\n")
