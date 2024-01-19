"""Common test fixtures and helper functions."""

import configparser
import logging
import os
import pathlib
from contextlib import contextmanager
from datetime import datetime
from typing import Generator

import pytest
from _pytest.logging import LogCaptureHandler
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.engine import create_engine, Engine
from testcontainers.postgres import PostgresContainer

from charging_stations_pipelines.models import Base
from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station

SOURCE_ID_COUNTER = 0


def get_incremented_counter() -> int:
    """Return an incremented counter."""
    global SOURCE_ID_COUNTER
    SOURCE_ID_COUNTER += 1
    return SOURCE_ID_COUNTER


def setup_temp_db() -> Engine:
    """Set up a temporary database for testing."""
    postgres_container = PostgresContainer("kartoza/postgis")
    postgres_container.start()
    sql_url = postgres_container.get_connection_url()
    engine: Engine = create_engine(sql_url)
    Base.metadata.create_all(engine)
    return engine


def get_config() -> configparser.RawConfigParser:
    """Get the config file."""
    config: configparser = configparser.RawConfigParser()
    current_dir = os.path.join(pathlib.Path(__file__).parent.resolve())
    config.read(os.path.join(os.path.join(current_dir, "config", "config.ini")))
    return config


def create_station() -> Station:
    """Create a station object."""
    station = Station()
    station.country_code = "DE"
    station.source_id = get_incremented_counter()
    station.operator = "Comsysto"
    station.data_source = "Test"
    station.point = from_shape(Point(float(1), float(1)))
    station.date_created = datetime(2012, 3, 3, 10, 10, 10)

    station.address = create_address()
    station.charging = create_charging()
    return station


def create_address() -> Address:
    """Create an address object."""
    address = Address()
    address.street = "Teststr."
    address.town = "Testhausen"
    address.postcode = "12345"
    address.district = ""
    address.state = ""
    address.country = ("DE",)
    return address


def create_charging() -> Charging:
    """Create a charging object."""
    charging = Charging()
    charging.station_id = 1
    charging.capacity = 1
    charging.kw_list = [11.0]
    charging.ampere_list = []
    charging.volt_list = []
    charging.socket_type_list = []
    charging.dc_support = False
    charging.total_kw = 11.0
    charging.max_kw = None
    return charging


def skip_if_github():
    """Checks if the current workflow is running on GitHub."""
    return "GITHUB_WORKFLOW" in os.environ


class LogLocalCaptureFixture:
    """Provides access and control of log capturing."""

    def __init__(self):
        self.handler = LogCaptureHandler()

    @contextmanager
    def __call__(
        self, level: int, logger: logging.Logger
    ) -> Generator[None, None, None]:
        """Context manager that sets the level for capturing of logs."""
        orig_level = logger.level

        logger.setLevel(level)
        logger.addHandler(self.handler)
        try:
            yield self.handler
        finally:
            logger.setLevel(orig_level)
            logger.removeHandler(self.handler)

    @property
    def logs(self) -> list[str]:
        """List of log lines captured by the log handler."""
        return [record.getMessage() for record in self.handler.records]


@pytest.fixture
def local_caplog() -> Generator[LogLocalCaptureFixture, None, None]:
    """Fixture for capturing logs locally. Returns a generator object yielding a LogLocalCaptureFixture instance."""
    yield LogLocalCaptureFixture()
