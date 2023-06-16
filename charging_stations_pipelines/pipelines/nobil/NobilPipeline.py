import os
from pathlib import Path

from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.orm import Session

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.shared import download_file, load_json_file, reject_if


# Nobil is the name of the data provider for norwegian and swedish data
class NobilStation:
    def __init__(self, id, operator, position, created, updated, street, house_number, zipcode, city,
                 number_charging_points):
        self.id = id
        self.operator = operator
        self.position = position
        self.created = created
        self.updated = updated
        self.street = street
        self.house_number = house_number
        self.zipcode = zipcode
        self.city = city
        self.number_charging_points = number_charging_points

    def __repr__(self):
        return f"NobilStation(id={self.id}, operator={self.operator}, position={self.position}, created={self.created}, updated={self.updated}, street={self.street}, house_number={self.house_number}, zipcode={self.zipcode}, city={self.city})"


def _parse_json_data(json_data) -> list[NobilStation]:
    all_nobil_stations: list[NobilStation] = []
    for s in json_data['chargerstations']:
        csmd = s['csmd']
        nobil_station = NobilStation(csmd['id'], csmd['Operator'], csmd['Position'], csmd['Created'],
                                     csmd['Updated'], csmd['Street'], csmd['House_number'], csmd['Zipcode'],
                                     csmd['City'], csmd['Number_charging_points'])
        all_nobil_stations.append(nobil_station)
    return all_nobil_stations


def _extract_lon_lat_from_position(position: str) -> tuple[float, float]:
    position = position.replace("(", "").replace(")", "")
    lon, lat = position.split(",")
    return float(lon), float(lat)


def _map_station_to_domain(nobil_station: NobilStation, country_code: str) -> Station:
    new_station = Station()
    new_station.source_id = str(nobil_station.id)
    new_station.data_source = "NOBIL"
    new_station.operator = nobil_station.operator
    long, lat = _extract_lon_lat_from_position(nobil_station.position)
    new_station.point = from_shape(Point(float(long), float(lat)))
    new_station.date_created = nobil_station.created
    new_station.date_updated = nobil_station.updated
    new_station.country_code = country_code

    return new_station


def _map_address_to_domain(nobil_station: NobilStation) -> Address:
    new_address: Address = Address()
    new_address.street = nobil_station.street + " " + nobil_station.house_number
    new_address.town = nobil_station.city
    new_address.postcode = nobil_station.zipcode
    return new_address


def _map_charging_to_domain(nobil_station: NobilStation) -> Charging:
    new_charging: Charging = Charging()
    new_charging.capacity = nobil_station.number_charging_points
    return new_charging


def _load_datadump_and_write_to_target(path_to_target, country_code: str):
    nobil_api_key = os.getenv("NOBIL_APIKEY")
    link_to_datadump = f"https://nobil.no/api/server/datadump.php?apikey={nobil_api_key}&countrycode={country_code}&format=json&file=true"
    download_file(link_to_datadump, path_to_target)


class NobilPipeline:
    def __init__(self, session: Session, country_code: str, online: bool = False):
        self.session = session

        accepted_country_codes = ["NOR", "SWE"]
        reject_if(country_code not in accepted_country_codes, "Invalid country code ")
        self.country_code = country_code
        self.online: bool = online

    def run(self):
        path_to_target = Path(__file__).parent.parent.parent.parent.joinpath("data/" + self.country_code + "_gov.json")
        if self.online:
            _load_datadump_and_write_to_target(path_to_target, self.country_code)

        nobil_stations_as_json = load_json_file(path_to_target)
        all_nobil_stations = _parse_json_data(nobil_stations_as_json)

        for nobil_station in all_nobil_stations:
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