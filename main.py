import argparse
import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from charging_stations_pipelines import db_utils
from charging_stations_pipelines import settings
from charging_stations_pipelines.deduplication.merger import StationMerger
from charging_stations_pipelines.pipelines.ocm.ocm import OcmPipeline
from charging_stations_pipelines.pipelines.osm.osm import OsmPipeline
from charging_stations_pipelines.pipelines.pipeline_factory import pipeline_factory
from charging_stations_pipelines.settings import db_uri
from charging_stations_pipelines.shared import config
from charging_stations_pipelines.stations_data_export import stations_data_export
from testing import testdata

logger = logging.getLogger("charging_stations_pipelines.main")


def parse_args(args):
    valid_task_options = ["import", "merge", "export", "testdata"]
    valid_country_options = ["DE", "FR", "GB", "IT", "NOR", "SWE"]

    parser = argparse.ArgumentParser(
        description='eCharm can best be described as an electronic vehicle charging stations data integrator. '
                    'It imports data from different publicly available sources, converts it into a common format, '
                    'searches for duplicates in the different sources and merges the data (e.g. the attributes) '
                    'and exports the original or merged data to csv or geo-json.',
        epilog='Example: python main.py import merge --countries de it -v ',
    )

    parser.add_argument('tasks', choices=valid_task_options, nargs='+', metavar='<task>',
                        help='one or more tasks to perform. The following tasks are available: %(choices)s. '
                             'import retrieves data for the selected countries and stores them in the database. '
                             'merge searches for duplicates and merges attributes of duplicate stations. '
                             'export creates a data export for the specified countries in csv or geo-json format. '
                             'testdata is only intended to be used for development purposes.')
    parser.add_argument('-c', '--countries', choices=valid_country_options,
                        default=valid_country_options, nargs='+', type=str.upper, metavar='<country-code>',
                        help='specifies the countries for which to perform the given tasks. '
                             'The country-codes must be one or several of %(choices)s (case-insensitive). '
                             'If not specified, the given tasks are run for all available countries')
    parser.add_argument('-o', '--offline', action='store_true',
                        help='if set, use data for import that is already present on disk, '
                             'i.e. from previous runs of the import task. '
                             'If not set, the data will be retrieved online from the different data sources.')
    parser.add_argument('-d', '--delete_data', action='store_true',
                        help='for the import task, delete all station data before importing. '
                             'For the merge task, delete only merged station data and '
                             'reset merge status of original stations.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='makes eCharm verbose during operations. Useful for debugging.')

    return parser.parse_args(args)


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
        stations_data_export(get_db_engine(), country, is_merged=False, csv=True, all_countries=False)


if __name__ == "__main__":
    cli_args = parse_args(sys.argv[1:])

    if cli_args.verbose:
        app_logger = logging.getLogger("charging_stations_pipelines")
        app_logger.setLevel(logging.DEBUG)
        for handler in app_logger.handlers:
            handler.setLevel(logging.DEBUG)

    for task in cli_args.tasks:
        logger.info("Running task " + task)
        if task == "import":
            run_import(cli_args.countries, not cli_args.offline, cli_args.delete_data)

        if task == "merge":
            run_merge(cli_args.countries, cli_args.delete_data)

        if task == "testdata":
            testdata.run()

        if task == "export":
            run_export(cli_args.countries)
