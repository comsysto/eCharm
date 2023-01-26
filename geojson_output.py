import geopandas as gpd

def convert_to_geojson(db_connection, country_code: str):

    get_stations_list_sql = f"""
        SELECT s.id as station_id, coordinates, data_source, operator, a.street, a.town FROM stations s
            LEFT JOIN address a ON s.id = a.station_id
            WHERE country_code='{country_code}' AND NOT is_merged
    """
    gdf: gpd.GeoDataFrame = gpd.read_postgis(get_stations_list_sql,
                                             con=db_connection, geom_col="coordinates")
    print(len(gdf))
    json_data = gdf.to_json()
    with open(f"stations_{country_code}.geo.json", "w") as outfile:
        outfile.write(json_data)





