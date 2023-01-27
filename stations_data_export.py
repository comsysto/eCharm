import geopandas as gpd


def stations_data_export(db_connection, country_code: str, all: bool=False, csv: bool=False):

    select_by_country = f"country_code='{country_code}' AND "
    if all:
        select_by_country = ""
        country_code = "europe"

    get_stations_list_sql = f"""
        SELECT s.id as station_id, coordinates, data_source, operator, a.street, a.town FROM stations s
            LEFT JOIN address a ON s.id = a.station_id
            WHERE {select_by_country}NOT is_merged
    """.format(select_by_country=select_by_country)

    gdf: gpd.GeoDataFrame = gpd.read_postgis(get_stations_list_sql,
                                             con=db_connection, geom_col="coordinates")
    print(len(gdf))

    if csv:
        suffix = "csv"
        gdf['latitude'] = gdf['coordinates'].apply(lambda point: point.y)
        gdf['longitude'] = gdf['coordinates'].apply(lambda point: point.x)
        json_data = gdf.to_csv()
    else:
        suffix = "geo.json"
        json_data = gdf.to_json()

    filename = f"stations_{country_code}.{suffix}"
    print(f"writing to {filename}")
    with open(filename, "w") as outfile:
        outfile.write(json_data)





