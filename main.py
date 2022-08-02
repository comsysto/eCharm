# import for the pipeline
import json
import os
import pathlib

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from mapping.charging import map_charging_ocm, map_charging_osm
from mapping.stations import (
    map_address_ocm,
    map_address_osm,
    map_station_osm,
    map_station_ocm
)
from models.station import Station
from pipelines._osm import OsmPipeline
from settings import db_uri


def ocm_pipeline():
    # Read excel file as pandas dataframe
    df = pd.read_csv(r"data/ocm_stations.csv")

    # df_mapped = map_address(df_dropped)
    engine = create_engine(db_uri, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    for index, row in tqdm(df.iterrows()):
        mapped_station: Station = map_station_ocm(row)
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

    with open("data/osm_stations.json") as osmStations:
        data = json.load(osmStations)
        for element in data["elements"]:
            station: Station = map_station_osm(element)
            session.add(station)
            address = map_address_osm(element, station.id)
            if address is not None:
                session.add(address)
            charging = map_charging_osm(element, station.id)
            session.add(charging)
            try:
                session.commit()
            except Exception as e:
                print(e)
                session.rollback()


if __name__ == "__main__":
    current_dir = os.path.join(pathlib.Path(__file__).parent.resolve())
    import configparser

    config: configparser = configparser.RawConfigParser()
    config.read(os.path.join(os.path.join(current_dir, "config", "config.ini")))

    # bna: BnaPipeline = BnaPipeline(
    #     config=config,
    #     db_session=sessionmaker(bind=(create_engine(db_uri, echo=True)))(),
    #     offline=True,
    # )
    # bna.run()
    osm: OsmPipeline = OsmPipeline(
        config=config,
        session=sessionmaker(bind=(create_engine(db_uri, echo=True)))(),
        offline=True,
    )
    osm.run()
    print("")
