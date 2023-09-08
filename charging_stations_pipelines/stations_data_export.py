import logging

import geopandas as gpd

from charging_stations_pipelines import settings

logger = logging.getLogger(__name__)


def stations_data_export(db_connection, country_code: str, is_merged: bool = False, all_countries: bool = False,
                         csv: bool = False):
    select_by_country = f"country_code='{country_code}' AND "
    if all_countries:
        select_by_country = ""
        country_code = "europe"

    select_merged = ""
    file_suffix_merged = "merged"
    if not is_merged:
        select_merged = "NOT "
        file_suffix_merged = "w_duplicates"

    prefix = settings.db_table_prefix
    get_stations_list_sql = f"""
        SELECT s.id as station_id, point, data_source, operator, a.street, a.town FROM {prefix}stations s
            LEFT JOIN {prefix}address a ON s.id = a.station_id
            WHERE {select_by_country}{select_merged}s.is_merged
    """

    gdf: gpd.GeoDataFrame = gpd.read_postgis(get_stations_list_sql,
                                             con=db_connection, geom_col="point")

    if csv:
        suffix = "csv"
        gdf['latitude'] = gdf['point'].apply(lambda point: point.y)
        gdf['longitude'] = gdf['point'].apply(lambda point: point.x)
        json_data = gdf.to_csv()
    else:
        suffix = "geo.json"
        json_data = gdf.to_json()

    filename = f"stations_{country_code}_{file_suffix_merged}.{suffix}"
    logger.info(f"writing {len(gdf)} stations to {filename}")
    with open(filename, "w") as outfile:
        outfile.write(json_data)
