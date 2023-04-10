import configparser
import json
import os
import pathlib
from typing import Dict, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from charging_stations_pipelines.mapping.charging import map_charging_osm
from charging_stations_pipelines.mapping.stations import map_address_osm, map_station_osm
from charging_stations_pipelines.utils.logging_utils import log
from charging_stations_pipelines.utils.osm_receiver import get_osm_data


class OsmPipeline:
    def __init__(self, country_code: str, config: configparser, session: Session, offline: bool = False):
        self.country_code = country_code
        self.config = config
        self.session = session
        self.offline: bool = offline
        self.data: Optional[Dict] = None

    def _retrieve_data(self):
        data_dir: str = os.path.join(
            pathlib.Path(__file__).parent.resolve(), "../..", "data"
        )
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_file_path = os.path.join(data_dir, self.config["OSM"]["filename"])
        if not self.offline:
            get_osm_data(self.country_code, tmp_file_path)
        with open(tmp_file_path, "r") as f:
            self.data = json.load(f)

    def run(self):
        self._retrieve_data()
        entry: Dict
        for entry in self.data.get("elements", []):
            mapped_address = map_address_osm(entry, None)
            mapped_charging = map_charging_osm(entry, None)
            mapped_station = map_station_osm(entry, self.country_code)
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            self.session.add(mapped_station)
            try:
                self.session.commit()
                self.session.flush()
            except IntegrityError as e:
                log.error(f"OSM-Entry exists already! Error: {e}")
                self.session.rollback()
                continue
            except Exception as e:
                log.error(f"OSM-Pipeline failed to run! Error: {e}")
                self.session.rollback()
