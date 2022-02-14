#import for the pipeline
import pandas as pd
from mapping.stations import map_address
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
from sqlalchemy.sql import text

#Read excel file as pandas dataframe
df = pd.read_excel(r'Ladesaeulenregister.xlsx')
#df = pd.read_csv('stations.csv', engine='python')
#set column 4 as header
df.columns = df.iloc[4]

df_dropped = df[5:]

#df_mapped = map_address(df_dropped)

engine = create_engine(db_uri, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

for index, row in tqdm(df_dropped.iterrows()):
    net_stations = create_station(row)
    session.add(new_stations)
    try:
        session.commit()
    except:
        session.rollback()


