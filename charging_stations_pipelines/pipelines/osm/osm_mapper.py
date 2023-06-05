import logging
from datetime import datetime
from typing import Dict, Optional

from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.shared import check_coordinates

logger = logging.getLogger(__name__)


def map_station_osm(entry: Dict, country_code: str):
    datasource: str = "OSM"
    lat = check_coordinates(entry["lat"])
    lon = check_coordinates(entry["lon"])
    operator: Optional[str] = entry["tags"].get("operator")
    new_station = Station()
    new_station.country_code = country_code
    new_station.source_id = entry["id"]
    new_station.operator = operator
    new_station.data_source = datasource
    new_station.point = from_shape(Point(float(lon), float(lat)))
    new_station.date_created = entry.get("timestamp", datetime.now())
    return new_station


def map_address_osm(entry, station_id):
    if "tags" in entry:
        tags = entry["tags"]
        if (
                "addr:city" in tags
                and "addr:country" in tags
                and "addr:housenumber" in tags
                and "addr:postcode" in tags
                and "addr:street" in tags
        ):
            city = tags["addr:city"]
            country = tags["addr:country"]
            housenumber = tags["addr:housenumber"]
            postcode = tags["addr:postcode"]
            street = tags["addr:street"]
            map_address = Address()
            map_address.state_old = None
            map_address.station_id = station_id
            map_address.street = street + " " + housenumber
            map_address.town = city
            map_address.postcode = postcode
            map_address.district_old = None
            map_address.country = country
            return map_address
    return None


def map_charging_osm(station_id):
    charging = Charging()
    charging.station_id = station_id
    # TODO: read charging info
    return charging
