import os
import pathlib

from sqlalchemy import create_engine

from pipelines._merger import StationMerger
from settings import db_uri

if __name__ == "__main__":
    current_dir = os.path.join(pathlib.Path(__file__).parent.resolve())
    import configparser

    config: configparser = configparser.RawConfigParser()
    config.read(os.path.join(os.path.join(current_dir, "config", "config.ini")))

    # bna: BnaPipeline = BnaPipeline(
    #     config=config,
    #     session=sessionmaker(bind=(create_engine(db_uri, echo=True)))(),
    #     offline=True,
    # )
    # bna.run()
    # osm: OsmPipeline = OsmPipeline(
    #     config=config,
    #     session=sessionmaker(bind=(create_engine(db_uri, echo=True)))(),
    #     offline=True,
    # )
    # osm.run()

    # ocm: OcmPipeline = OcmPipeline(
    #     config=config,
    #     session=sessionmaker(bind=(create_engine(db_uri, echo=True)))(),
    #     offline=True,
    # )
    # ocm.run()

    StationMerger(config=config, con=create_engine(db_uri, echo=True)).retrieveData()

    print("")
