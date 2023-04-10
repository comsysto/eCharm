from unittest import TestCase

from geoalchemy2.shape import from_shape
from shapely import wkb
from shapely.speedups._speedups import Point
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from charging_stations_pipelines.models import Base
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines._merger import StationMerger
from test.shared import get_config, create_station


class TestStationMerger(TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.postgres_container = PostgresContainer("kartoza/postgis")
        self.postgres_container.start()
        sql_url = self.postgres_container.get_connection_url()
        self.engine = create_engine(sql_url)
        Base.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        super().tearDown()
        self.postgres_container.stop()

    def test_expect_a_merged_entry_if_two_duplicates_exists(self):
        # given: setup db with two duplicated station entries
        session = sessionmaker(bind=self.engine)()

        station_one = create_station()
        station_one.data_source = "BNA"
        station_duplicate = create_station()
        station_duplicate.data_source = "OSM"

        session.add(station_one)
        session.add(station_duplicate)
        session.commit()

        # when: run the merger
        station_merger = StationMerger(country_code='DE', config=(get_config()), con=self.engine, is_test=False)
        station_merger.run()

        # then: the two duplicates are merged
        stations = session.query(Station).all()
        session.close()
        self.assertEqual(3, len(stations))
        self.assertEqual("is_duplicate", station_one.merge_status)
        self.assertEqual("is_duplicate", station_duplicate.merge_status)
        self.assertFalse(station_one.is_merged)
        self.assertFalse(station_duplicate.is_merged)
        merged_stations = list(filter(lambda s: s.is_merged is True, stations))
        self.assertEqual(1, len(merged_stations))

    def test_OCM_should_have_higher_prio_than_BNA(self):
        # given: setup db with two duplicated station entries
        session = sessionmaker(bind=self.engine)()

        station_bna = create_station()
        station_bna.data_source = "BNA"
        station_bna.point = from_shape(Point(float(1.1111111223), float(1.11111123)))
        station_ocm = create_station()
        expected_x = 1.11111112
        expected_y = 1.111111
        station_ocm.point = from_shape(Point(float(expected_x), float(expected_y)))
        station_ocm.data_source = "OCM"

        session.add(station_bna)
        session.add(station_ocm)
        session.commit()

        # when: run the merger
        station_merger = StationMerger(country_code='DE', config=(get_config()), con=self.engine, is_test=False)
        station_merger.run()

        # then: the two duplicates are merged
        stations = session.query(Station).all()
        session.close()
        merged_stations = list(filter(lambda s: s.is_merged is True, stations))
        self.assertEqual(1, len(merged_stations))
        merged_station = merged_stations[0]
        point = wkb.loads(bytes(merged_station.point.data))
        self.assertEqual(expected_x, point.x)
        self.assertEqual(expected_y, point.y)