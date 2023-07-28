from logging import Logger

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from charging_stations_pipelines.models.station import Station


class StationTableUpdater:
    def __init__(self, session: Session, logger: Logger):
        self.session = session
        self.logger = logger
        self.counts = {
            'new': 0,
            'updated': 0,  # no update mechanism yet
            'error': 0
        }

    def update_station(self, station: Station, data_source_key: str):
        error_occurred = False
        self.session.add(station)

        try:
            self.session.commit()
            self.session.flush()
        except IntegrityError as e:
            error_occurred = True
            self.logger.debug(f"{data_source_key}-Entry exists already! Error: {e}")
            self.session.rollback()
        except Exception as e:
            error_occurred = True
            self.logger.error(f"{data_source_key}-Entry -- Unexpected Error: {e}")
            self.session.rollback()

        if error_occurred:
            self.counts['error'] += 1
        else:
            self.counts['new'] += 1

    def log_update_station_counts(self):
        self.logger.info(f"new stations: {self.counts['new']}, "
                         f"updated stations: {self.counts['updated']}, "
                         f"errors: {self.counts['error']}")
