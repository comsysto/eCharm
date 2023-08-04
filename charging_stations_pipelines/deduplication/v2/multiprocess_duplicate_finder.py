import logging
from concurrent.futures import ProcessPoolExecutor

from charging_stations_pipelines.deduplication.v2.ratio_calculator import RatioCalculator
from charging_stations_pipelines.deduplication.v2.types import StationDuplicate, Station

logger = logging.getLogger(__name__)

THRESHOLD_SAME_SOURCE = 0.999
THRESHOLD_DIFFERENT_SOURCE = 0.95
NUMBER_OF_PROCESSES = 10


class MultiProcessDuplicateFinder:
    def __init__(self):
        self.delta_calculator = RatioCalculator()

    def run(self, all_stations):

        # Divide the objects into subsets for each process
        stations_per_process = len(all_stations) // NUMBER_OF_PROCESSES
        subsets = [all_stations[i:i + stations_per_process] for i in range(0, len(all_stations), stations_per_process)]

        equal_pairs: list[StationDuplicate] = []

        with ProcessPoolExecutor(max_workers=NUMBER_OF_PROCESSES) as executor:

            futures = []

            for i, subset in enumerate(subsets):
                futures.append(executor.submit(self.find_duplicates, i, subset, all_stations))

            for future in futures:
                equal_pairs.extend(future.result())

        clusters: [set] = []
        for pair in equal_pairs:

            already_in_cluster = False
            for cluster in clusters:
                if pair.station in cluster or pair.duplicate in cluster:
                    cluster.add(pair.station)
                    cluster.add(pair.duplicate)
                    already_in_cluster = True
                    break
            if not already_in_cluster:
                cluster = set()
                cluster.add(pair.station)
                cluster.add(pair.duplicate)
                clusters.append(cluster)

        cleaned_duplicates: list[StationDuplicate] = []
        for cluster in clusters:
            count = 0
            base_station = None
            for station in cluster:
                count += 1
                if count == 1:
                    base_station = station
                    continue
                cleaned_duplicates.append(StationDuplicate(base_station, station, 1.0))

        return cleaned_duplicates

    def find_duplicates(self, process_id: int, subset: list[Station], all_stations: list[Station]):

        logger.info(f"Process {process_id} started with {len(subset)} stations")
        count = 0

        total_station_with_duplicates: list[StationDuplicate] = []

        for station in subset:
            count += 1
            station_with_duplicates: list[StationDuplicate] = []

            if count % 50 == 0:
                logger.info(f"Process {process_id}: Processing station {count}")

            for s in all_stations:

                if station == s:
                    continue

                ratio = self.delta_calculator.ratio(station, s)

                if s.data_source == station.data_source and ratio > THRESHOLD_SAME_SOURCE:
                    station_with_duplicates.append(StationDuplicate(station, s, ratio))
                if s.data_source != station.data_source and ratio > THRESHOLD_DIFFERENT_SOURCE:
                    station_with_duplicates.append(StationDuplicate(station, s, ratio))

            for duplicate in station_with_duplicates:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"ratio: {duplicate.ratio}")
                    logger.debug(f"station: {duplicate.station}")
                    logger.debug(f"duplicate: {duplicate.duplicate}")

            total_station_with_duplicates.extend(station_with_duplicates)

        return total_station_with_duplicates
