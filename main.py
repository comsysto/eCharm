import logging
import os
import pathlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from charging_stations_pipelines.pipelines._bna import BnaPipeline
from charging_stations_pipelines.pipelines._france import FraPipeline
from charging_stations_pipelines.pipelines._gbgov import GbPipeline
from charging_stations_pipelines.pipelines._ocm import OcmPipeline
from charging_stations_pipelines.pipelines._osm import OsmPipeline
from charging_stations_pipelines.settings import db_uri
from charging_stations_pipelines.stations_data_export import stations_data_export

logger = logging.getLogger("charging_stations_pipelines.main")

if __name__ == "__main__":
    country_code = "GB"
    current_dir = os.path.join(pathlib.Path(__file__).parent.resolve())
    import configparser

    config: configparser = configparser.RawConfigParser()
    config.read(os.path.join(os.path.join(current_dir, "config", "config.ini")))

    logger.debug("Selected country code: " + country_code)

    if country_code == "DE":
        bna: BnaPipeline = BnaPipeline(
            config=config,
            session=sessionmaker(bind=(create_engine(db_uri)))(),
            offline=True,
        )
        bna.run()

    elif country_code == "FR":
        fra: FraPipeline = FraPipeline(
            config=config,
            session=sessionmaker(bind=(create_engine(db_uri)))(),
            offline=True,
        )
        fra.run()

    elif country_code == "GB":
        gb: GbPipeline = GbPipeline(
            country_code=country_code,
            config=config,
            session=sessionmaker(bind=(create_engine(db_uri)))(),
            offline=False,
        )
        gb.run()    

    osm: OsmPipeline = OsmPipeline(
        country_code=country_code,
        config=config,
        session=sessionmaker(bind=(create_engine(db_uri)))(),
        offline=False,
    )
    osm.run()

    ocm: OcmPipeline = OcmPipeline(
        country_code=country_code,
        config=config,
        session=sessionmaker(bind=(create_engine(db_uri)))(),
        offline=False,
    )
    ocm.run()


    from charging_stations_pipelines.pipelines._merger import StationMerger
    is_test = False
    merger: StationMerger = StationMerger(country_code=country_code, config=config, con=create_engine(db_uri, pool_pre_ping=True), is_test=is_test)
    merger.run()


    #testdata.run()

    stations_data_export(create_engine(db_uri), country_code, is_merged=False, csv=True, all_countries=False)