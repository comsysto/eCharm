"""Main entry point for eCharm."""

import argparse
import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from charging_stations_pipelines import db_utils, settings
from charging_stations_pipelines.deduplication.merger import StationMerger
from charging_stations_pipelines.pipelines.ocm import SCOPE_COUNTRIES as ocm_countries
from charging_stations_pipelines.pipelines.ocm.ocm import OcmPipeline
from charging_stations_pipelines.pipelines.osm import SUPPORTED_COUNTRIES as osm_countries
from charging_stations_pipelines.pipelines.osm.osm import OsmPipeline
from charging_stations_pipelines.pipelines.pipeline_factory import pipeline_factory
from charging_stations_pipelines.settings import config, db_uri
from charging_stations_pipelines.stations_data_export import (
    ExportArea,
    stations_data_export,
)
from testing import testdata

logger = logging.getLogger("charging_stations_pipelines.main")


def parse_args(args):
    """This method is used to parse the command line arguments for the eCharm program."""
    valid_task_options = ["import", "merge", "export", "testdata"]
    # FIXME: add all European countries flag
    valid_country_options = ["DE", "AT", "FR", "GB", "IT", "NOR", "SWE"]
    valid_export_format_options = ["csv", "GeoJSON"]

    parser = argparse.ArgumentParser(
        description="eCharm can best be described as an electronic vehicle charging stations data integrator. "
        "It imports data from different publicly available sources, converts it into a common format, "
        "searches for duplicates in the different sources and merges the data (e.g. the attributes) "
        "and exports the original or merged data to csv or GeoJSON.",
        epilog="Example: python main.py import merge --countries de it -v ",
    )

    parser.add_argument(
        "tasks",
        choices=valid_task_options,
        nargs="+",
        metavar="<task>",
        help="one or more tasks to perform. The following tasks are available: %(choices)s. "
        "import retrieves data for the selected countries and stores them in the database. "
        "merge searches for duplicates and merges attributes of duplicate stations. "
        "export creates a data export for the specified countries in csv or geo-json format. "
        "testdata is only intended to be used for development purposes.",
    )
    parser.add_argument(
        "-c",
        "--countries",
        choices=valid_country_options,
        default=valid_country_options,
        nargs="+",
        type=lambda s: s.upper(),
        metavar="<country-code>",
        help="specifies the countries for which to perform the given tasks. "
        "The country-codes must be one or several of %(choices)s (case-insensitive). "
        "If not specified, the given tasks are run for all available countries",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="makes eCharm verbose during operations. Useful for debugging.",
    )

    group_import_merge = parser.add_argument_group("import and merge options")
    group_import_merge.add_argument(
        "-o",
        "--offline",
        action="store_true",
        help="use data for import that is already present on disk, "
        "i.e. from previous runs of the import task. "
        "The default is to retrieve data online from the different data sources.",
    )
    group_import_merge.add_argument(
        "-d",
        "--delete_data",
        action="store_true",
        help="for the import task, delete all station data before importing. "
        "For the merge task, delete only merged station data and "
        "reset merge status of original stations.",
    )
    group_export = parser.add_argument_group("export options")
    group_export.add_argument(
        "--export_file_descriptor",
        action="store",
        metavar="<file descriptor>",
        help="custom descriptor to be used for export file name. Default is to use the country "
        "code of the corresponding export.",
    )
    group_export.add_argument(
        "--export_format",
        choices=valid_export_format_options,
        default="csv",
        help="specifies the format of exported data. Default is csv format.",
    )
    group_export.add_argument(
        "--export_charging",
        action="store_true",
        help="export additional charging attributes for stations. Default is to export only "
        "geo-location, address and operator.",
    )
    group_export.add_argument(
        "--export_merged_stations",
        action="store_true",
        help="export only the stations merged by the eCharm merge step. Default is to "
        "export only the stations that were imported from original data sources.",
    )
    group_export.add_argument(
        "--export_all_countries",
        action="store_true",
        help="ignore the countries option for the export, and export all countries "
        "station data into one single file. Default is to export to one file per country.",
    )
    group_export.add_argument(
        "--export_area",
        nargs=3,
        type=float,
        metavar=("<lon>", "<lat>", "<radius in m>"),
        help="export data in a circular area around the given lon/lat coordinates.",
    )

    return parser.parse_args(args)


def get_db_engine(**kwargs):
    """Method to get the database engine."""
    connect_args = {"options": f"-csearch_path={settings.db_schema},public"}
    return create_engine(name_or_url=db_uri, connect_args=connect_args, **kwargs)


def run_import(countries: list[str], online: bool, delete_data: bool):
    """This method runs the import process for the specified countries. It has three parameters."""
    if delete_data:
        logger.info("Deleting all data...")
        db_utils.delete_all_data(sessionmaker(bind=get_db_engine())())
        logger.info("Finished deleting all data.")

    logger.info("Starting to import GOV data...")
    # FIXME: externalize all loops
    # FIXME: add all countries flag
    for country in countries:
        logger.info(f"Importing GOV data for country: {country}...")
        db_session = sessionmaker(bind=get_db_engine())()

        # FIXME!
        gov_pipeline = pipeline_factory(db_session, country, online)
        # gov_pipeline.run()
    logger.info("Finished importing GOV data.")

    # FIXME: externalize all loops
    logger.info("Starting to import OSM data...")
    num_countries = len(osm_countries)
    logger.info(f"Importing OSM data for {len(osm_countries)} countries")
    for idx, country in enumerate(osm_countries, 1):
        logger.info(f"[{idx}/{num_countries}] Importing OSM data for country: {country}...")
        db_session = sessionmaker(bind=get_db_engine())()

        # FIXME
        osm = OsmPipeline(country, config, db_session, online)
        # osm.run()
    logger.info("Finished importing OSM data.")

    # FIXME
    logger.info("Starting to import OCM data...")
    num_countries = len(ocm_countries)
    logger.info(f"Importing OCM data for {num_countries} countries")
    for idx, country in enumerate(ocm_countries, 1):
        logger.info(f"[{idx}/{num_countries}] Importing OCM data for country: {country}...")
        db_session = sessionmaker(bind=get_db_engine())()

        # FIXME
        ocm = OcmPipeline(country, config, db_session, online)
        ocm.run()
    logger.info("Finished importing OCM data")


def run_merge(countries: list[str], delete_data: bool):
    """Run the merge process for the specified countries."""
    engine = get_db_engine(pool_pre_ping=True)

    if delete_data:
        logger.info("Deleting merged data...")
        db_utils.delete_all_merged_data(sessionmaker(bind=engine)())
        logger.info("Finished deleting merged data.")

    logger.info("Starting to merge data...")
    for country in countries:
        logger.info(f"Merging data for country: {country}...")
        merger = StationMerger(country_code=country, config=config, db_engine=engine)
        merger.run()
    logger.info("Finished merging data.")


def run_export(cli_args):
    """This method runs the export process based on the provided command line arguments."""
    args_file_descriptor = (
        cli_args.export_file_descriptor if cli_args.export_file_descriptor else ""
    )

    args_export_area = ExportArea(
            lon=cli_args.export_area[0],
            lat=cli_args.export_area[1],
            radius_meters=cli_args.export_area[2],
        ) if cli_args.export_area else None


    should_export_csv = cli_args.export_format == "csv"

    if cli_args.export_all_countries or args_export_area:
        stations_data_export(
            get_db_engine(),
            country_code="",
            export_merged=cli_args.export_merged_stations,
            export_charging_attributes=cli_args.export_charging,
            export_to_csv=should_export_csv,
            export_all_countries=True,
            export_area=args_export_area,
            file_descriptor=args_file_descriptor,
        )
    else:
        for country in cli_args.countries:
            # noinspection PyArgumentEqualDefault
            stations_data_export(
                get_db_engine(),
                country_code=country,
                export_merged=cli_args.export_merged_stations,
                export_charging_attributes=cli_args.export_charging,
                export_to_csv=should_export_csv,
                file_descriptor=args_file_descriptor,
            )


def setup_logging(is_verbose: bool):
    if is_verbose:
        app_logger = logging.getLogger("charging_stations_pipelines")
        app_logger.setLevel(logging.DEBUG)
        for handler in app_logger.handlers:
            handler.setLevel(logging.DEBUG)


def main():
    """Entry point for the application."""
    command_line_args = parse_args(sys.argv[1:])
    setup_logging(command_line_args.verbose)

    tasks = {
        "import": lambda args: run_import(
            args.countries, not args.offline, args.delete_data
        ),
        "merge": lambda args: run_merge(args.countries, args.delete_data),
        "testdata": lambda args: testdata.run(),
        "export": run_export,
    }

    logger.info("Starting eCharm...")
    logger.info(
        f"Specified {len(command_line_args.tasks)} tasks: "
        + ", ".join(command_line_args.tasks)
    )
    for task_name in command_line_args.tasks:
        logger.info(f"Running task: {task_name}")
        task_function = tasks.get(task_name)
        if task_function:
            task_function(command_line_args)

    logger.info("Finished eCharm.")


if __name__ == "__main__":
    main()
