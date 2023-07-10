import geopandas as gpd
from geopandas.io.sql import _df_to_geodf
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from charging_stations_pipelines.settings import db_uri


def main():
    engine = create_engine(db_uri)

    country_code = "DE"

    query = text("""
    select merged_station.point as source_location, duplicate_station.point as target_location
        from stations merged_station, merged_station_source m, stations duplicate_station
        where merged_station.id = m.merged_station_id and duplicate_station.id = m.duplicate_station_id
        and merged_station.country_code = :country_code and duplicate_station.country_code = :country_code
        """)

    with engine.connect() as connection:
        gdf: gpd.GeoDataFrame = gpd.read_postgis(query, con=connection, geom_col="source_location",
                                                 params={"country_code": country_code})
        gdf = _df_to_geodf(gdf, "target_location")
        gdf['source_location_latitude'] = gdf['source_location'].apply(lambda point: point.y)
        gdf['source_location_longitude'] = gdf['source_location'].apply(lambda point: point.x)
        gdf['target_location_latitude'] = gdf['target_location'].apply(lambda point: point.y)
        gdf['target_location_longitude'] = gdf['target_location'].apply(lambda point: point.x)
        json_data = gdf.to_csv()

        filename = f"merged_stations_sources_{country_code}.csv"
        with open(filename, "w") as outfile:
            outfile.write(json_data)


if __name__ == '__main__':
        main()
