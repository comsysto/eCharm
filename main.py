import getopt
import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from charging_stations_pipelines.deduplication.merger import StationMerger
from charging_stations_pipelines.pipelines.ocm.ocm import OcmPipeline
from charging_stations_pipelines.pipelines.osm.osm import OsmPipeline
from charging_stations_pipelines.pipelines.pipeline_factory import pipeline_factory
from charging_stations_pipelines.settings import db_uri
from charging_stations_pipelines.shared import reject_if, config, string_to_bool
from charging_stations_pipelines.stations_data_export import stations_data_export
from charging_stations_pipelines import settings
from charging_stations_pipelines import db_utils

from testing import testdata

logger = logging.getLogger("charging_stations_pipelines.main")


class CommandLineArguments:
    tasks = []
    countries = []
    online: bool = True

    def __init__(self, argv) -> None:
        super().__init__()
        self.argv = argv
        # explicitly set defaults
        self.delete_data = False
        self.online = False

        try:
            opts, args = getopt.getopt(argv[1:], "ht:c:o:", ["help", "tasks=", "countries=", "online=", "delete_data="])
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
                self.online = string_to_bool(arg)
            elif opt in ("-d", "--delete_data"):
                self.delete_data = string_to_bool(arg)
        self.validate()

    def print_help(self):
        arg_help = "{0} --tasks=<task1,task2> --countries=<country1,country2> --online=<true/false> " \
                   "--delete_data=<true/false>".format(self.argv[0])
        print(arg_help)
        print("Example: python main.py --countries=de,it --tasks=import --online=true")
        sys.exit(2)

    def validate(self):
        if not self.tasks or not self.countries:
            self.print_help()

        accepted_tasks = ["import", "merge", "testdata", "export"]
        reject_if(not all(t in accepted_tasks for t in self.tasks), "Invalid task")

        accepted_countries = ["DE", "FR", "GB", "IT", "NOR", "SWE"]
        reject_if(not all(t in accepted_countries for t in self.countries), "Invalid country")


def get_db_engine(**kwargs):
    connect_args = {"options": f"-csearch_path={settings.db_schema},public"}
    return create_engine(name_or_url=db_uri, connect_args=connect_args, **kwargs)


def run_import(countries, online: bool, delete_data: bool):

    if delete_data:
        db_utils.delete_all_data(sessionmaker(bind=get_db_engine())())

    for country in countries:
        db_session = sessionmaker(bind=get_db_engine())()
        gov_pipeline = pipeline_factory(db_session, country, online)
        gov_pipeline.run()

        osm: OsmPipeline = OsmPipeline(country, config, db_session, online)
        osm.run()

        ocm: OcmPipeline = OcmPipeline(country, config, db_session, online)
        ocm.run()


def run_merge(countries, delete_data: bool):
    engine = get_db_engine(pool_pre_ping=True)

    if delete_data:
        print("deleting merged data ...")
        db_utils.delete_all_merged_data(sessionmaker(bind=engine)())

    for country in countries:
        merger: StationMerger = StationMerger(country_code=country, config=config, db_engine=engine)
        merger.run()


def run_export(countries):
    for country in countries:
        stations_data_export(create_engine(db_uri), country, is_merged=False, csv=True, all_countries=False)


if __name__ == "__main__":
    command_line_arguments = CommandLineArguments(sys.argv)

    for task in command_line_arguments.tasks:
        logger.debug("Running task " + task)
        if task == "import":
            run_import(command_line_arguments.countries, command_line_arguments.online,
                       command_line_arguments.delete_data)

        if task == "merge":
            run_merge(command_line_arguments.countries, command_line_arguments.delete_data)

        if task == "testdata":
            testdata.run()

        if task == "export":
            run_export(command_line_arguments.countries)
