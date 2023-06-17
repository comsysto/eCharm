from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from charging_stations_pipelines.pipelines.de.bna import BnaPipeline
from charging_stations_pipelines.pipelines.fr.france import FraPipeline
from charging_stations_pipelines.pipelines.gb.gbgov import GbPipeline
from charging_stations_pipelines.pipelines.nobil.NobilPipeline import NobilPipeline
from charging_stations_pipelines.settings import db_uri
from charging_stations_pipelines.shared import config


class EmptyPipeline:
    def run(self):
        pass


def pipeline_factory(country="DE", online: bool = True):
    db_session = sessionmaker(bind=(create_engine(db_uri)))()
    pipelines = {
        "DE": BnaPipeline(config, db_session, online),
        "FR": FraPipeline(config, db_session, online),
        "GB": GbPipeline(config, db_session, online),
        "NOR": NobilPipeline(db_session, "NOR", online),
        "SWE": NobilPipeline(db_session, "SWE", online),
    }
    if country in pipelines:
        return pipelines[country]
    else:
        return EmptyPipeline()
