import configparser
import json
import logging
import os
import pathlib
from typing import Dict, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from charging_stations_pipelines.pipelines.osm.osm_mapper import map_address_osm, map_charging_osm, map_station_osm
from charging_stations_pipelines.pipelines.osm.osm_receiver import get_osm_data

logger = logging.getLogger(__name__)


class OsmPipeline:
    def __init__(self, country_code: str, config: configparser, session: Session, online: bool = False):
        self.country_code = country_code
        self.config = config
        self.session = session
        self.online: bool = online
        self.data: Optional[Dict] = None

    def _retrieve_data(self):
        data_dir: str = os.path.join(
            pathlib.Path(__file__).parent.resolve(), "../../..", "data"
        )
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_file_path = os.path.join(data_dir, self.config["OSM"]["filename"])
        if self.online:
            get_osm_data(self.country_code, tmp_file_path)
        with open(tmp_file_path, "r") as f:
            self.data = json.load(f)

    def run(self):
        self._retrieve_data()
        entry: Dict
        for entry in self.data.get("elements", []):
            mapped_address = map_address_osm(entry, None)
            mapped_charging = map_charging_osm(entry)
            mapped_station = map_station_osm(entry, self.country_code)
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            self.session.add(mapped_station)
            try:
                self.session.commit()
                self.session.flush()
            except IntegrityError as e:
                logger.error(f"OSM-Entry exists already! Error: {e}")
                self.session.rollback()
                continue
            except Exception as e:
                logger.error(f"OSM-Pipeline failed to run! Error: {e}")
                self.session.rollback()
