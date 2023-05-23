import configparser
import os
from pathlib import Path

from sqlalchemy.orm import Session

from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.shared import download_file, load_json_file


class NorwayNobilStation:
    def __init__(self, attribute1, attribute2):
        self.attribute1 = attribute1
        self.attribute2 = attribute2


def _parse_json_data(json_data) -> list[NorwayNobilStation]:
    all_norway_stations: list[NorwayNobilStation] = []
    for s in json_data:
        attribute1 = s['attribute1']
        attribute2 = s['attribute2']
        norway_station = NorwayNobilStation(attribute1, attribute2)
        all_norway_stations.append(norway_station)
    return all_norway_stations


def _map_to_model(norway_station: NorwayNobilStation) -> Station:
    pass


def _load_datadump_and_write_to_target(path_to_target):
    nobil_api_key = os.getenv("NOBIL_APIKEY")
    link_to_datadump = f"https://nobil.no/api/server/datadump.php?apikey={nobil_api_key}&countrycode=NOR&format=json&file=true"
    download_file(link_to_datadump, path_to_target)


class NorPipeline:
    def __init__(self, config: configparser, session: Session, online: bool = False):
        self.config = config
        self.session = session
        self.online: bool = online

    def run(self):
        path_to_target = Path(__file__).parent.parent.parent.parent.joinpath("data/norway.json")
        if self.online:
            _load_datadump_and_write_to_target(path_to_target)

        norway_stations_as_json = load_json_file(path_to_target)
        all_norway_stations = _parse_json_data(norway_stations_as_json)

        for norway_station in all_norway_stations:
            station: Station = _map_to_model(norway_station)
            # check if station already exists in db and add
            self.session.add(station)

        self.session.flush()
        self.session.commit()

