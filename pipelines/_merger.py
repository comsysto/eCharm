import configparser
import os

import geopandas as gpd
from sqlalchemy.orm import Session
from models.station import Station

class StationMerger:
    def __init__(self, config: configparser, session: Session):
        self.config = config
        self.session = session


    def retrieveData(self):
        #stations = self.session.query(Station).all()
        sql = 'SELECT * FROM stations'
        gdf = gpd.read_postgis(sql, con=self.session, geom_col="coordinates")
        gdf_dropped = gdf.drop(gdf[["date_created", "date_updated"]], axis=1)
        out_json = os.path.join('nyc_neighborhoods.geojson')
        gdf_dropped.to_file(out_json, driver="GeoJSON")

