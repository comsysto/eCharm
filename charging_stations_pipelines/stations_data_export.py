"""Exports stations data to a file."""

import logging
from dataclasses import dataclass
from typing import Optional

import geopandas as gpd

from charging_stations_pipelines import settings

logger = logging.getLogger(__name__)


@dataclass
class ExportArea:
    """Represents an area targeted for data export."""
    lon: float
    lat: float
    radius_meters: float


def stations_data_export(db_connection,
                         country_code: str,
                         export_merged: bool = False,
                         export_charging_attributes: bool = False,
                         export_all_countries: bool = False,
                         export_to_csv: bool = False,
                         export_area: Optional[ExportArea] = None,
                         file_descriptor: str = ""):
    """Exports stations data to a file."""
    logger.info(f"Exporting stations data for country {country_code}")
    country_filter = f"country_code='{country_code}' AND " if country_code != "" and not export_all_countries else ""
    merged_filter = "s.is_merged" if export_merged else "NOT s.is_merged"
    export_area_filter = (f" AND ST_Dwithin("
                          f"point, "
                          f"ST_MakePoint({export_area.lon}, {export_area.lat}, 4326)::geography, "
                          f"{export_area.radius_meters}"
                          f")") \
        if export_area else ""

    logger.info(f"Using stations filter: country_filter='{country_filter}', "
                f"merged_filter='{merged_filter}', export_area_filter='{export_area_filter}'")

    get_stations_filter = f"{country_filter}{merged_filter}{export_area_filter}"

    prefix = settings.db_table_prefix
    # FIXME: export country_code too!
    if not export_charging_attributes:
        get_stations_list_sql = f"""
            SELECT s.id as station_id, point, data_source, operator, a.street, a.town 
            FROM {prefix}stations s
            LEFT JOIN {prefix}address a ON s.id = a.station_id
            WHERE {get_stations_filter}
        """
    else:
        get_stations_list_sql = f"""
            SELECT s.id as station_id, point, data_source, operator, 
                a.street, a.town,
                c.capacity, c.socket_type_list, c.dc_support, c.total_kw,  c.max_kw
            FROM {prefix}stations s
            LEFT JOIN {prefix}address a ON s.id = a.station_id
            LEFT JOIN {prefix}charging c ON s.id = c.station_id
            WHERE {get_stations_filter}
        """

    logger.debug(f"Running postgis query {get_stations_list_sql}")
    gdf: gpd.GeoDataFrame = gpd.read_postgis(get_stations_list_sql, con=db_connection, geom_col="point")

    if export_to_csv:
        suffix = "csv"
        gdf['latitude'] = gdf['point'].apply(lambda point: point.y if point else None)
        gdf['longitude'] = gdf['point'].apply(lambda point: point.x if point else None)
        export_data = gdf.to_csv()
    else:
        suffix = "geo.json"
        export_data = gdf.to_json()

    logger.debug(f"Found stations of shape: {gdf.shape}")
    logger.debug(f"Data sample: {gdf.sample(5)}")

    file_country = "europe" if export_all_countries else country_code
    file_description = get_file_description(file_descriptor, file_country, export_area)
    file_suffix_merged = "merged" if export_merged else "w_duplicates"
    file_suffix_charging = "_w_charging" if export_charging_attributes else ""

    filename = f"stations_{file_description}_{file_suffix_merged}{file_suffix_charging}.{suffix}"
    logger.info(f"Writing {len(gdf)} stations to {filename}")
    with open(filename, "w") as outfile:
        outfile.write(export_data)
        logger.info(f"Done writing, file size: {outfile.tell()}")


def get_file_description(file_descriptor: str, file_country: str, export_circle: ExportArea):
    """Returns a file description based on the given parameters."""
    is_export_circle_specified = export_circle is not None
    if file_descriptor == "":
        if is_export_circle_specified:
            return f"{export_circle.lon}_{export_circle.lat}_{export_circle.radius_meters}"
        else:
            return file_country
    else:
        if is_export_circle_specified:
            return file_descriptor
        else:
            return f"{file_descriptor}_{file_country}"
