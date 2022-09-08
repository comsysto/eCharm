import configparser
import os

import geopandas as gpd


class StationMerger:
    def __init__(self, config: configparser, con):
        self.config = config
        self.con = con

    def retrieveData(self):
        sql = 'SELECT * FROM stations'
        gdf = gpd.read_postgis(sql, con=self.con, geom_col="coordinates")
        gdf_dropped = gdf.drop(gdf[["date_created", "date_updated"]], axis=1)
        out_json = os.path.join('nyc_neighborhoods.geojson')
        gdf_dropped.to_file(out_json, driver="GeoJSON")
        print("")
