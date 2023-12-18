"""This module contains the pipeline for the Nobil data provider."""

import configparser
import logging
import os
from _decimal import Decimal
from pathlib import Path
from typing import Final

from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.orm import Session
from tqdm import tqdm

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines import Pipeline
from ... import PROJ_DATA_DIR
from ...file_utils import download_file, load_json_file
from ...shared import JSON, reject_if

logger = logging.getLogger(__name__)


class NobilConnector:
    """This class represents a connector for the Nobil API."""

    def __init__(self, power_in_kw: Decimal):
        self.power_in_kw = power_in_kw


class NobilStation:
    """This class represents a station from the Nobil API."""

    def __init__(self, station_id, operator, position, created, updated, street, house_number, zipcode, city,
                 number_charging_points, connectors: list[NobilConnector]):
        self.station_id = station_id
        self.operator = operator
        self.position = position
        self.created = created
        self.updated = updated
        self.street = street
        self.house_number = house_number
        self.zipcode = zipcode
        self.city = city
        self.number_charging_points = number_charging_points
        self.connectors = connectors


def _parse_json_data(json_data: JSON) -> list[NobilStation]:
    all_nobil_stations: list[NobilStation] = []
    s: JSON
    for s in json_data['chargerstations']:
        attrs: JSON = s['attrs']
        parsed_connectors = parse_nobil_connectors(attrs['conn'])

        csmd: JSON = s['csmd']
        nobil_station = NobilStation(csmd['id'], csmd['Operator'], csmd['Position'], csmd['Created'],
                                     csmd['Updated'], csmd['Street'], csmd['House_number'], csmd['Zipcode'],
                                     csmd['City'], csmd['Number_charging_points'], parsed_connectors)
        all_nobil_stations.append(nobil_station)
    return all_nobil_stations


def parse_nobil_connectors(connectors: JSON) -> list[NobilConnector]:
    """Parse the connectors of a nobil station."""
    parsed_connectors: list[NobilConnector] = []
    # iterate over all connectors and add them to the station
    for k, v in connectors.items():
        charging_capacity: str = v['5']['trans']  # contains a string like "7,4 kW - 230V 1-phase max 32A" or "75 kW DC"

        # extract the power in kW from the charging capacity string
        power_in_kw = Decimal(charging_capacity.split(" kW")[0].replace(",", ".")) \
            if " kW" in charging_capacity else None

        parsed_connectors.append(NobilConnector(power_in_kw))
    return parsed_connectors


def _extract_lon_lat_from_position(position: str) -> tuple[float, float]:
    position = position.replace("(", "").replace(")", "")
    lon, lat = position.split(",")

    return float(lon), float(lat)


def _map_station_to_domain(nobil_station: NobilStation, country_code: str) -> Station:
    station = Station()
    station.source_id = str(nobil_station.station_id)
    station.data_source = "NOBIL"
    station.operator = nobil_station.operator
    lat, long = _extract_lon_lat_from_position(nobil_station.position)
    station.point = from_shape(Point(float(long), float(lat)))
    station.date_created = nobil_station.created
    station.date_updated = nobil_station.updated
    station.country_code = country_code

    return station


def _map_address_to_domain(nobil_station: NobilStation) -> Address:
    address: Address = Address()
    address.street = nobil_station.street + " " + nobil_station.house_number
    address.town = nobil_station.city
    address.postcode = nobil_station.zipcode

    return address


def _map_charging_to_domain(nobil_station: NobilStation) -> Charging:
    charging: Charging = Charging()
    charging.capacity = nobil_station.number_charging_points
    charging.kw_list = [connector.power_in_kw for connector in nobil_station.connectors
                        if connector.power_in_kw is not None]
    if len(charging.kw_list) > 0:
        charging.max_kw = max(charging.kw_list)
        charging.total_kw = sum(charging.kw_list)

    return charging


def _load_datadump_and_write_to_target(out_file: Path, country_code: str) -> None:
    nobil_api_key = os.getenv("NOBIL_APIKEY")
    link_to_datadump = (f"https://nobil.no/api/server/datadump.php?apikey="
                        f"{nobil_api_key}&countrycode={country_code}&format=json&file=true")

    download_file(link_to_datadump, out_file)


class NobilPipeline(Pipeline):
    """This class represents the pipeline for the Nobil data provider."""

    def __init__(self, session: Session, country_code: str, config: configparser, online: bool = False):
        super().__init__(config, session, online)

        self.country_code = country_code.upper()

        accepted_country_codes = ["NOR", "SWE"]
        reject_if(self.country_code not in accepted_country_codes, "Invalid country code ")

        self.data_path: Final[Path] = PROJ_DATA_DIR / f"{self.country_code}_gov.json"
        self.data_path.parent.mkdir(parents=True, exist_ok=True)

    def retrieve_data(self) -> None:
        if self.online:
            logger.info("Retrieving Online Data")
            _load_datadump_and_write_to_target(self.data_path, self.country_code)

        self.data = load_json_file(self.data_path)

    def run(self):
        logger.info("Running NOR/SWE GOV Pipeline...")
        self.retrieve_data()

        all_nobil_stations = _parse_json_data(self.data)
        for nobil_station in tqdm(iterable=all_nobil_stations, total=len(all_nobil_stations)):
            station: Station = _map_station_to_domain(nobil_station, self.country_code)
            address: Address = _map_address_to_domain(nobil_station)
            charging: Charging = _map_charging_to_domain(nobil_station)

            station.address = address
            station.charging = charging

            # check if station already exists in db and add
            existing_station = self.session.query(Station).filter_by(source_id=station.source_id).first()
            if existing_station is None:
                self.session.add(station)

        self.session.flush()
        self.session.commit()
