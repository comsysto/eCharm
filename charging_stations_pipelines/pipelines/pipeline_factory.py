"""Factory for creating pipelines based on the country code."""

from sqlalchemy.orm import Session

from charging_stations_pipelines.pipelines import Pipeline
from charging_stations_pipelines.pipelines.at.econtrol import EcontrolAtPipeline
from charging_stations_pipelines.pipelines.de.bna import BnaPipeline
from charging_stations_pipelines.pipelines.fr.france import FraPipeline
from charging_stations_pipelines.pipelines.gb.gbgov import GbPipeline
from charging_stations_pipelines.pipelines.nobil.NobilPipeline import NobilPipeline
from charging_stations_pipelines.shared import config


class EmptyPipeline(Pipeline):
    """Represents an empty pipeline."""

    def __init__(self):
        super().__init__(None, None)

    def run(self):
        """Do nothing."""
        pass


def pipeline_factory(db_session: Session, country="DE", online=True) -> Pipeline:
    """Creates a pipeline based on the country code."""
    pipelines = {
        "AT":  EcontrolAtPipeline(config, db_session, online),
        "DE":  BnaPipeline(config, db_session, online),
        "FR":  FraPipeline(config, db_session, online),
        "GB":  GbPipeline(config, db_session, online),
        "NOR": NobilPipeline(db_session, "NOR", online),
        "SWE": NobilPipeline(db_session, "SWE", online),
    }

    return pipelines[country] if country in pipelines else EmptyPipeline()
