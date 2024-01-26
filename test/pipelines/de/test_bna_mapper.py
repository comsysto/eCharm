"""Unit tests for the mapper of the BNA pipeline."""

import hashlib

import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.de import (
    bna_mapper as de_mapper,
    DATA_SOURCE_KEY,
)
from charging_stations_pipelines.shared import float_cmp_eq


def test_map_station_bna():
    # Pandas Series to simulate the row
    data_row = pd.Series(
        {
            "Breitengrad": "48.7758459",
            "Längengrad": "9.1829321",  # NOSONAR
            "Betreiber": "Test Operator",
            "Inbetriebnahmedatum": pd.Timestamp(2022, 1, 1),
        }
    )

    station = de_mapper.map_station_bna(data_row)

    assert isinstance(station, Station)
    assert station.country_code == "DE"
    assert station.data_source == DATA_SOURCE_KEY
    assert (
        station.source_id
        == hashlib.sha256((data_row["Breitengrad"] + data_row["Längengrad"] + station.data_source).encode()).hexdigest()
    )
    assert station.operator == data_row["Betreiber"]
    assert station.point == from_shape(Point(float(data_row["Längengrad"]), float(data_row["Breitengrad"])))
    assert station.date_created == data_row["Inbetriebnahmedatum"].strftime("%Y-%m-%d")


def test_map_address_bna():
    # Testing when str length of "Ort" >= 2 and str length of "Postleitzahl" == 5
    row = pd.Series(
        {
            "Postleitzahl": " 12345 ",
            "Ort": " Test-Town ",
            "Straße": " Test-Street ",
            "Hausnummer": " 12A ",
            "Kreis/kreisfreie Stadt": "Test-District",
            "Bundesland": "Test-State",
        }
    )

    station_id = 1
    address = de_mapper.map_address_bna(row, station_id)

    assert address.station_id == station_id
    assert address.street == "Test-Street 12A"
    assert address.town == "Test-Town"
    assert address.postcode == "12345"
    assert address.district == "Test-District"
    assert address.state == "Test-State"
    assert address.country == "DE"

    # Testing when str length of "Ort" < 2 and str length of "Postleitzahl" not being 5
    row = pd.Series(
        {
            "Postleitzahl": "123",
            "Ort": "T",
            "Straße": " Test-Street ",
            "Hausnummer": " 12A ",
            "Kreis/kreisfreie Stadt": "Test-District",
            "Bundesland": "Test-State",
        }
    )

    station_id = 2
    address = de_mapper.map_address_bna(row, station_id)

    assert isinstance(address, Address)
    assert address.station_id == station_id
    assert address.street == "Test-Street 12A"
    assert address.town is None
    assert address.postcode is None
    assert address.district == "Test-District"
    assert address.state == "Test-State"
    assert address.country == "DE"


def test_map_charging_bna():
    # Pandas Series to simulate the row
    row = pd.Series({"Nennleistung Ladeeinrichtung [kW]": "5.0", "Anzahl Ladepunkte": 2})

    charging = de_mapper.map_charging_bna(row, 1)

    assert isinstance(charging, Charging)
    assert charging.station_id == 1
    assert float_cmp_eq(charging.total_kw, 5.0)
    assert charging.capacity == 2
    assert charging.kw_list == []  # no "P" in keys
    assert charging.max_kw is None  # as kw_list is []
