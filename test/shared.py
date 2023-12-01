import configparser
import os
import pathlib
from datetime import datetime

from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.engine import Engine, create_engine
from testcontainers.postgres import PostgresContainer

from charging_stations_pipelines.models import Base
from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station

SOURCE_ID_COUNTER = 0


def get_incremented_counter() -> int:
    global SOURCE_ID_COUNTER
    SOURCE_ID_COUNTER += 1
    return SOURCE_ID_COUNTER


def setup_temp_db() -> Engine:
    postgres_container = PostgresContainer("kartoza/postgis")
    postgres_container.start()
    sql_url = postgres_container.get_connection_url()
    engine: Engine = create_engine(sql_url)
    Base.metadata.create_all(engine)
    return engine


def get_config() -> configparser.RawConfigParser:
    config: configparser = configparser.RawConfigParser()
    current_dir = os.path.join(pathlib.Path(__file__).parent.resolve())
    config.read(os.path.join(os.path.join(current_dir, "config", "config.ini")))
    return config


def create_station() -> Station:
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
    address = Address()
    address.street = "Teststr."
    address.town = "Testhausen"
    address.postcode = "12345"
    address.district = ""
    address.state = ""
    address.country = ("DE",)
    return address


def create_charging() -> Charging:
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


def skip_if_github(func):
    def wrapper(*args, **kwargs):
        if os.getenv("GITHUB_WORKFLOW") is not None:
            print(f"Skipped test {func.__name__} because it is running on Github Actions")
            return
        return func(*args, **kwargs)

    return wrapper
