"""Unit tests for the mapper of the BNA pipeline."""

import hashlib
from io import StringIO

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

# This data corresponds to some real world entries from the BNA Excel file
test_data_csv = """Betreiber;Straße;Hausnummer;Adresszusatz;Postleitzahl;Ort;Bundesland;Kreis/kreisfreie Stadt;Breitengrad;Längengrad;Inbetriebnahmedatum;Nennleistung Ladeeinrichtung [kW];Art der Ladeeinrichung;Anzahl Ladepunkte;Steckertypen1;P1 [kW];Public Key1;Steckertypen2;P2 [kW];Public Key2;Steckertypen3;P3 [kW];Public Key3;Steckertypen4;P4 [kW];Public Key4
Test Operator 1;Test-Street-1;1a;Parkhaus;12345;Test-Town-1;Test-State-1;Test-District-1;48.482237;9.911473;12/12/2019;30;Normalladeeinrichtung;2;AC Steckdose Typ 2, AC Schuko;22;;AC Steckdose Typ 2, AC Schuko;22
Test Operator 1;Test-Street-2;2;;12345;Test-Town-1;Test-State-1;Test-District-1;48.46334554;9.947873333;12/10/2021;150;Schnellladeeinrichtung;2;DC Kupplung Combo;150;;DC Kupplung Combo;150
Test Operator 1;Test-Street-2;2;;12345;Test-Town-1;Test-State-1;Test-District-1;48.46334554;9.947873333;12/10/2021;300;Schnellladeeinrichtung;2;DC Kupplung Combo;300;;DC Kupplung Combo;300
Test Operator 2;Test-Street-3;3;;23456;Test-Town-2;Test-State-2;Test-District-2;49.4544;12.4113;5/11/2019;93;Schnellladeeinrichtung;2;AC Kupplung Typ 2;43;;AC Kupplung Typ 2, DC CHAdeMO;50
Test Operator 2;Test-Street-3;3;;23456;Test-Town-2;Test-State-2;Test-District-2;49.4544;12.4113;12/12/2022;150;Schnellladeeinrichtung;3;DC Kupplung Combo;150;3056301006072A8648CE3D020106052B8104000A0342000444E6B925417BC8655D07524A1628519E3B8F6DB5FB12DD833ABB9D64F7A7EFDF73BFAF36BC18DB8F37646A0EDA19655E12CAC0FDF9BCE9A21D0992AA23C890AD;DC Kupplung Combo;150;3056301006072A8648CE3D020106052B8104000A034200044053A381729A208AF001A3B191C3354568FAF8A2DDF80062415481E69CE39B5B54F89A6B40E25F55BC64D8FE93D6F25A035C550E3643C2677D34DE69725BB224;AC Steckdose Typ 2;22;3056301006072A8648CE3D020106052B8104000A03420004F05E168067CA5403C6F57507A4F6EC98C5E92618A03FDF678418420D826B752C6402E23CD626371990F5793770880F715C986522370781FE591FEAC9EC417397
Test Operator 2;Test-Street-4;4-9;;34567;Test-Town-3;Test-State-2;Test-District-3;50.03301;10.11196;12/12/2022;150;Schnellladeeinrichtung;3;DC Kupplung Combo;150;3056301006072A8648CE3D020106052B8104000A0342000444E6B925417BC8655D07524A1628519E3B8F6DB5FB12DD833ABB9D64F7A7EFDF73BFAF36BC18DB8F37646A0EDA19655E12CAC0FDF9BCE9A21D0992AA23C890AD;DC Kupplung Combo;150;3056301006072A8648CE3D020106052B8104000A034200044053A381729A208AF001A3B191C3354568FAF8A2DDF80062415481E69CE39B5B54F89A6B40E25F55BC64D8FE93D6F25A035C550E3643C2677D34DE69725BB224;AC Steckdose Typ 2;22;3056301006072A8648CE3D020106052B8104000A03420004F05E168067CA5403C6F57507A4F6EC98C5E92618A03FDF678418420D826B752C6402E23CD626371990F5793770880F715C986522370781FE591FEAC9EC417397
Test Operator 2;Test-Street-5;5;4282;45678;Test-Town-4;Test-State-2;Test-District-4;49.09276;12.72605;25/1/2023;88;Normalladeeinrichtung;4;AC Steckdose Typ 2;22;;AC Steckdose Typ 2;22;;AC Steckdose Typ 2;22;;AC Steckdose Typ 2;22;
Test Operator 2;Test-Street-5;5;4282;45678;Test-Town-4;Test-State-2;Test-District-4;49.09276;12.72605;25/1/2023;88;Normalladeeinrichtung;4;AC Steckdose Typ 2;22;;AC Steckdose Typ 2;22;;AC Steckdose Typ 2;22;;AC Steckdose Typ 2;22;
Test Operator 2;Test-Street-5;5;4282;45678;Test-Town-4;Test-State-2;Test-District-4;49.09276;12.72605;25/1/2023;44;Normalladeeinrichtung;2;AC Steckdose Typ 2;22;;AC Steckdose Typ 2;22;;;;;;;
Test Operator 3;Test-Street-6;6/1;;67890;Test-Town-5;Test-State-1;Test-District-5;48.90052;9.417098;16/7/2021;39.6;Normalladeeinrichtung;2;DC Kupplung Combo;22;;AC Steckdose Typ 2;11;;;;;;;
Test Operator 3;Test-Street-7;7;;78901;Test-Town-5;Test-State-1;Test-District-5;48.41428;9.47985;1/6/2014;3.6;Normalladeeinrichtung;2;AC Steckdose Typ 2;3.6;;AC Schuko;2.3;;;;;;;
    """

test_pd_series_old = pd.Series(
    {
        "Betreiber": "Test Operator",
        "Straße": "Test-Street",
        "Hausnummer": "12A",
        "Postleitzahl": "12345",
        "Ort": "Test-Town",
        "Bundesland": "Test-State",
        "Kreis/kreisfreie Stadt": "Test-District",
        "Breitengrad": "48.7758459",
        "Längengrad": "9.1829321",
        "Inbetriebnahmedatum": pd.Timestamp(2022, 1, 1),
        "Nennleistung Ladeeinrichtung [kW]": "5.0",
        "Art der Ladeeinrichung": "Scnellladeeinrichtung",
        "Anzahl Ladepunkte": 2,
    }
)


def test_transform_raw_bna_data():
    df = pd.read_csv(StringIO(test_data_csv), sep=";")
    print(df.to_string())
    new_df = de_mapper.transform_raw_bna_data(df)
    print(new_df.to_string())
    print(df.dtypes)


def test_map_station_bna():
    station = de_mapper.map_station_bna(test_pd_series_old)

    assert isinstance(station, Station)
    assert station.country_code == "DE"
    assert station.data_source == DATA_SOURCE_KEY
    assert (
        station.source_id
        == hashlib.sha256(
            (test_pd_series_old["Breitengrad"] + test_pd_series_old["Längengrad"] + station.data_source).encode()
        ).hexdigest()
    )
    assert station.operator == test_pd_series_old["Betreiber"]
    assert station.point == from_shape(
        Point(float(test_pd_series_old["Längengrad"]), float(test_pd_series_old["Breitengrad"]))
    )
    assert station.date_created == test_pd_series_old["Inbetriebnahmedatum"].strftime("%Y-%m-%d")


def test_map_address_bna():
    # Testing when str length of "Ort" >= 2 and str length of "Postleitzahl" == 5
    station_id = 1
    address = de_mapper.map_address_bna(test_pd_series_old, station_id)

    assert address.station_id == station_id
    assert address.street == "Test-Street 12A"
    assert address.town == "Test-Town"
    assert address.postcode == "12345"
    assert address.district == "Test-District"
    assert address.state == "Test-State"
    assert address.country == "DE"

    # Testing when str length of "Ort" < 2 and str length of "Postleitzahl" not being 5
    invalid_data_row = pd.Series(
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
    address = de_mapper.map_address_bna(invalid_data_row, station_id)

    assert isinstance(address, Address)
    assert address.station_id == station_id
    assert address.street == "Test-Street 12A"
    assert address.town is None
    assert address.postcode is None
    assert address.district == "Test-District"
    assert address.state == "Test-State"
    assert address.country == "DE"


def test_map_charging_bna():
    charging = de_mapper.map_charging_bna(test_pd_series_old, 1)

    assert isinstance(charging, Charging)
    assert charging.station_id == 1
    assert float_cmp_eq(charging.total_kw, 5.0)
    assert charging.capacity == 2
    assert charging.kw_list == []  # no "P" in keys
    assert charging.max_kw is None  # as kw_list is []
