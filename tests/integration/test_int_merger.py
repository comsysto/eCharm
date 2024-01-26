"""Integration Tests for the merger module."""

import pandas as pd
import pytest
from geoalchemy2.shape import from_shape
from shapely import wkb
from shapely.geometry import Point
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.ddl import CreateSchema
from testcontainers.postgres import PostgresContainer

from charging_stations_pipelines import settings
from charging_stations_pipelines.deduplication.merger import StationMerger
from charging_stations_pipelines.models import Base
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.at import econtrol_mapper as at_mapper
from charging_stations_pipelines.shared import float_cmp_eq
from test.shared import create_station, get_config


@pytest.fixture
def postgres_container():
    """Start a postgres container for testing."""
    container = PostgresContainer("kartoza/postgis")
    container.start()
    yield container
    container.stop()


@pytest.fixture
def engine(postgres_container):
    """Create a database engine for testing."""
    sql_url = postgres_container.get_connection_url()
    connect_args = {"options": f"-csearch_path={settings.db_schema},public"}
    db_engine = create_engine(sql_url, connect_args=connect_args)
    db_engine.execute(CreateSchema(settings.db_schema))
    Base.metadata.create_all(db_engine)
    return db_engine


def _set_up_db(engine, stations):
    """Set up the database with the given stations."""
    session = sessionmaker(bind=engine)()
    for station in stations:
        session.add(station)
    session.commit()
    return session


def _check_merger(engine, create_stations, run_merger, check_results):
    # Given: db with two duplicate station entries
    session = _set_up_db(engine, create_stations())
    # When: run the merger
    run_merger(engine)
    # Then: two duplicate stations are merged
    check_results(session)


def _run_merger(engine):
    """Merge duplicate stations."""

    # Suppressing Pandas warning (1/2): "A value is trying to be set on a copy of a slice from a DataFrame."
    pd.options.mode.chained_assignment = None  # default: 'warn'

    station_merger = StationMerger(country_code="DE", config=(get_config()), db_engine=engine)
    station_merger.run()

    # Suppressing Pandas warning (2/2): restoring default value
    pd.options.mode.chained_assignment = "warn"


@pytest.mark.integration_test
def test_int_deduplication_expect_a_merged_entry_if_two_duplicates_exists(engine):
    def _create_stations():
        # Given: two duplicate stations
        station_one = create_station()
        station_one.data_source = "BNA"
        station_one.source_id = "BNA_ID1"

        station_duplicate = create_station()
        station_duplicate.data_source = "OSM"
        station_duplicate.source_id = "OSM_ID1"

        return station_one, station_duplicate

    def _check_results(session):
        # Check that all_stations are merged
        # noinspection DuplicatedCode
        all_stations: list[Station] = session.query(Station).all()
        assert len(all_stations) == 3

        not_merged_stations = [s for s in all_stations if not s.is_merged]
        assert len(not_merged_stations) == 2
        assert all(s.merge_status == "is_duplicate" for s in not_merged_stations)

        merged_stations = [s for s in all_stations if s.is_merged]
        assert len(merged_stations) == 1
        merged_station: Station = merged_stations[0]
        merged_station_source_stations: list[Station] = merged_station.source_stations
        assert len(merged_station_source_stations) == 2
        assert merged_station_source_stations[0].duplicate_source_id == "OSM_ID1"
        assert merged_station_source_stations[1].duplicate_source_id == "BNA_ID1"

        session.close()

    _check_merger(engine, _create_stations, _run_merger, _check_results)


@pytest.mark.integration_test
def test_int_deduplication_ocm_should_have_higher_prio_than_bna(engine):
    def _create_stations():
        # Given: two duplicate stations
        station_bna = create_station()
        station_bna.data_source = "BNA"
        station_bna.point = from_shape(Point(float(1.1111111223), float(1.11111123)))

        station_ocm = create_station()
        station_ocm.point = from_shape(Point(float(1.11111112), float(1.111111)))
        station_ocm.data_source = "OCM"

        return station_bna, station_ocm

    def _check_results(session):
        # Check that all_stations are merged
        all_stations: list[Station] = session.query(Station).all()
        merged_stations = [s for s in all_stations if s.is_merged]
        assert len(merged_stations) == 1

        merged_station = merged_stations[0]
        assert merged_station.data_source == "OCM"

        point = wkb.loads(bytes(merged_station.point.data))
        expected_x, expected_y = 1.11111112, 1.111111
        assert float_cmp_eq(point.x, expected_x)
        assert float_cmp_eq(point.y, expected_y)

        session.close()

        _check_merger(engine, _create_stations, _run_merger, _check_results)


@pytest.mark.integration_test
def test_int_at_merger_bug_country_code_data_source_mismatch(engine):
    def _create_test_station(raw: pd.Series, country_code: str) -> Station:
        station = at_mapper.map_station(raw, country_code)

        station.address = at_mapper.map_address(raw, country_code, station.id)
        station.charging = at_mapper.map_charging(raw, station.id)

        return station

    def _create_stations():
        # Given: two problematic stations
        # noinspection DuplicatedCode
        raw1: pd.Series = pd.Series(
            {
                "evseCountryId": "DE",
                "evseOperatorId": "ELE",
                "evseStationId": "EKRIMML4",
                "status": "ACTIVE",
                "label": "KRIMML",
                "description": None,
                "postCode": 5743,
                "city": "Krimml",
                "street": "Gerlos Straße, Parkplatz P4",
                "location": {"latitude": 12.167938, "longitude": 12.167938},
                "distance": None,
                "contactName": "David Gruber",
                "telephone": "+4369917057801",
                "email": "david@elektroauto.at",
                "website": "www.elektroauto.at",
                "directions": "Ober dem letzten Parkplatz Krimmler Wasserfälle",
                "greenEnergy": 1.0,
                "freeParking": 1.0,
                "openingHours": {"text": None, "details": []},
                "priceUrl": None,
                "points": [],
                "public": True,
            }
        )

        # noinspection DuplicatedCode
        raw2: pd.Series = pd.Series(
            {
                "evseCountryId": "DE",
                "evseOperatorId": "ELE",
                "evseStationId": "EKRIMML",
                "status": "ACTIVE",
                "label": "KRIMML",
                "description": None,
                "postCode": 5743,
                "city": "Krimml",
                "street": "Gerlos Straße, Parkplatz P4",
                "location": {"latitude": 12.167938, "longitude": 12.167938},
                "distance": None,
                "contactName": "David Gruber",
                "telephone": "+4369917057801",
                "email": "david@elektroauto.at",
                "website": "www.elektroauto.at",
                "directions": "Ober dem letzten Parkplatz Krimmler Wasserfälle",
                "greenEnergy": 1.0,
                "freeParking": 1.0,
                "openingHours": {"text": None, "details": []},
                "priceUrl": None,
                "points": [],
                "public": True,
            }
        )

        return _create_test_station(raw1, "AT"), _create_test_station(raw2, "AT")

    # Given: db with two problematic station entries
    session = _set_up_db(engine, _create_stations())

    # When: run the merger
    # Merge duplicate stations

    # Suppressing Pandas warning (1/2): "A value is trying to be set on a copy of a slice from a DataFrame."
    pd.options.mode.chained_assignment = None  # default: 'warn'

    station_merger = StationMerger(country_code="AT", config=(get_config()), db_engine=engine)
    station_merger.run()

    # Suppressing Pandas warning (2/2): restoring default value
    pd.options.mode.chained_assignment = "warn"

    # Check that all_stations are merged
    # noinspection DuplicatedCode
    all_stations: list[Station] = session.query(Station).all()
    assert len(all_stations) == 3

    not_merged_stations = [s for s in all_stations if not s.is_merged]
    assert len(not_merged_stations) == 2
    assert all(s.merge_status == "is_duplicate" for s in not_merged_stations)

    merged_stations = [s for s in all_stations if s.is_merged]
    assert len(merged_stations) == 1

    merged_station: Station = merged_stations[0]
    merged_station_source_stations: list[Station] = merged_station.source_stations
    assert len(merged_station_source_stations) == 2

    session.close()
