import os
import pathlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from settings import db_uri

if __name__ == "__main__":
    current_dir = os.path.join(pathlib.Path(__file__).parent.resolve())
    import configparser

    config: configparser = configparser.RawConfigParser()
    config.read(os.path.join(os.path.join(current_dir, "config", "config.ini")))

    from pipelines._bna import BnaPipeline

    bna: BnaPipeline = BnaPipeline(
        config=config,
        session=sessionmaker(bind=(create_engine(db_uri, echo=True)))(),
        offline=True,
    )
    bna.run()

    from pipelines._osm import OsmPipeline

    osm: OsmPipeline = OsmPipeline(
        config=config,
        session=sessionmaker(bind=(create_engine(db_uri, echo=True)))(),
        offline=True,
    )
    osm.run()

    from pipelines._ocm import OcmPipeline

    ocm: OcmPipeline = OcmPipeline(
        config=config,
        session=sessionmaker(bind=(create_engine(db_uri, echo=True)))(),
        offline=True,
    )
    ocm.run()

    # from pipelines._merger import StationMerger
    # StationMerger(config=config, con=create_engine(db_uri, echo=True)).retrieveData()

    print("")
