import configparser
import getopt
import logging
import os
import pathlib
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from charging_stations_pipelines.deduplication.merger import StationMerger
from charging_stations_pipelines.pipelines.bna import BnaPipeline
from charging_stations_pipelines.pipelines.france import FraPipeline
from charging_stations_pipelines.pipelines.gbgov import GbPipeline
from charging_stations_pipelines.pipelines.ocm import OcmPipeline
from charging_stations_pipelines.pipelines.osm import OsmPipeline
from charging_stations_pipelines.settings import db_uri
from charging_stations_pipelines.shared import reject_if
from charging_stations_pipelines.stations_data_export import stations_data_export
from testing import testdata

logger = logging.getLogger("charging_stations_pipelines.main")

current_dir = os.path.join(pathlib.Path(__file__).parent.resolve())
config: configparser = configparser.RawConfigParser()
config.read(os.path.join(os.path.join(current_dir, "config", "config.ini")))


class CommandLineArguments:
    tasks = []
    countries = []
    online: bool = True

    def __init__(self, argv) -> None:
        super().__init__()
        self.argv = argv

        try:
            opts, args = getopt.getopt(argv[1:], "ht:c:o:", ["help", "tasks=", "countries=", "online="])
        except Exception:
            logger.exception("Could not parse arguments")
            raise

        for opt, arg in opts:
            if opt in ("-h", "--help"):
                self.print_help()
            elif opt in ("-t", "--tasks"):
                self.tasks = arg.split(",")
            elif opt in ("-c", "--countries"):
                self.countries = [c.upper() for c in arg.split(",")]
            elif opt in ("-o", "--online"):
                self.online = bool(arg)
        self.validate()

    def print_help(self):
        arg_help = "{0} --tasks=<task1,task2> --countries=<country1,country2> --online=<online>".format(self.argv[0])
        print(arg_help)
        print("Example: python main.py --countries=de,it --tasks=import --online=true")
        sys.exit(2)

    def validate(self):
        if not self.tasks or not self.countries:
            self.print_help()

        accepted_tasks = ["import", "merge", "testdata", "export"]
        reject_if(not all(t in accepted_tasks for t in self.tasks), "Invalid task")

        accepted_countries = ["DE", "FR", "GB", "IT"]
        reject_if(not all(t in accepted_countries for t in self.countries), "Invalid country")


def run_import(countries, online):
    db_session = sessionmaker(bind=(create_engine(db_uri)))()
    if "DE" in countries:
        bna: BnaPipeline = BnaPipeline(
            config=config,
            session=db_session,
            offline=online,
        )
        bna.run()
    elif "FR" in countries:
        fra: FraPipeline = FraPipeline(
            config=config,
            session=db_session,
            offline=online,
        )
        fra.run()

    elif "GB" in countries:
        gb: GbPipeline = GbPipeline(
            country_code="GB",
            config=config,
            session=db_session,
            offline=online,
        )
        gb.run()

    for country in countries:
        osm: OsmPipeline = OsmPipeline(
            country_code=country,
            config=config,
            session=db_session,
            offline=online,
        )
        osm.run()

        ocm: OcmPipeline = OcmPipeline(
            country_code=country,
            config=config,
            session=db_session,
            offline=online,
        )
        ocm.run()


def run_merge(countries):
    engine = create_engine(db_uri, pool_pre_ping=True)
    for country in countries:
        merger: StationMerger = StationMerger(country_code=country, config=config, con=engine)
        merger.run()


def run_export(countries):
    for country in countries:
        stations_data_export(create_engine(db_uri), country, is_merged=False, csv=True, all_countries=False)


if __name__ == "__main__":

    command_line_arguments = CommandLineArguments(sys.argv)

    for task in command_line_arguments.tasks:
        logger.debug("Running task " + task)
        if task == "import":
            run_import(command_line_arguments.countries, command_line_arguments.online)

        if task == "merge":
            run_merge(command_line_arguments.countries)

        if task == "testdata":
            testdata.run()

        if task == "export":
            run_export(command_line_arguments.countries)
