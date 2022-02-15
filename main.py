#import for the pipeline
import pandas as pd
from mapping.stations import map_address, map_stations_bna
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from geopandas.tools import geocode
from tqdm import tqdm
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from settings import db_uri
from models.station import Station
from models.address import Address
from models.charging import Charging
from sqlalchemy.sql import text



def bna_pipeline():
    # Read excel file as pandas dataframe
    df = pd.read_excel(r'data/bundesagentor_stations.xlsx')
    df.columns = df.iloc[9]

    #Drop the comments in the excel
    df_dropped = df[10:]

    # df_mapped = map_address(df_dropped)
    engine = create_engine(db_uri, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    for index, row in tqdm(df_dropped.iterrows()):
        mapped_station = map_stations_bna(row)
        session.add(mapped_station)
        try:
            session.commit()
        except:
            session.rollback()

if __name__ == '__main__':
    bna_pipeline()


'''

'''


