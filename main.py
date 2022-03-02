# import for the pipeline
import json

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from mapping.charging import map_charging_bna, map_charging_ocm, map_charging_osm
from mapping.stations import (
    map_address_bna,
    map_address_ocm,
    map_stations_bna,
    map_stations_ocm, map_station_osm, map_address_osm
)
from models.station import Station
from settings import db_uri


def bna_pipeline():
    # Read excel file as pandas dataframe
    df = pd.read_excel(r"data/bundesagentor_stations.xlsx")
    df.columns = df.iloc[9]

    # Drop the comments in the excel
    df_dropped = df[10:]

    df_dropped.drop_duplicates(subset=["Breitengrad", "Längengrad"], keep="first", inplace=True)

    # df_mapped = map_address(df_dropped)
    engine = create_engine(db_uri, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    for index, row in tqdm(df_dropped.iterrows()):
        mapped_address = map_address_bna(row, None)
        mapped_charging = map_charging_bna(row, None)
        mapped_station: Station = map_stations_bna(row)
        mapped_station.address = mapped_address
        mapped_station.charging = mapped_charging
        session.add(mapped_station)

        try:
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()


def ocm_pipeline():
    # Read excel file as pandas dataframe
    df = pd.read_csv(r"data/ocm_stations.csv")

    # df_mapped = map_address(df_dropped)
    engine = create_engine(db_uri, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    for index, row in tqdm(df.iterrows()):
        mapped_station: Station = map_stations_ocm(row)
        session.add(mapped_station)
        session.flush()
        mapped_address = map_address_ocm(row, mapped_station.id)
        session.add(mapped_address)
        mapped_charging = map_charging_ocm(row, mapped_station.id)
        session.add(mapped_charging)

        try:
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()


def osm_pipeline():
    engine = create_engine(db_uri, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    with open('data/osm_stations.json') as osmStations:
        data = json.load(osmStations)
        for element in data['elements']:
            station: Station = map_station_osm(element)
            session.add(station)
            session.flush()
            address = map_address_osm(element,station.id)
            if address is not None:
                session.add(address)
            charging = map_charging_osm(element,station.id)
            session.add(charging)
            try:
                session.commit()
            except Exception as e:
                print(e)
                session.rollback()


if __name__ == "__main__":
    # bna_pipeline()
    # ocm_pipeline()
    osm_pipeline()