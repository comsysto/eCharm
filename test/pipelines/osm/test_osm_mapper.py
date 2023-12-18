"""Unit tests for the mapper of the OSM pipeline."""
from datetime import datetime
from unittest.mock import patch, PropertyMock

import pytest
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.osm import DATA_SOURCE_KEY, osm_mapper
from charging_stations_pipelines.pipelines.osm.osm_mapper import (
    extract_kw_list,
    extract_kw_map,
)
from charging_stations_pipelines.shared import float_cmp_eq


@pytest.mark.parametrize("test_data, expected", [(
        {
            "id": 2144376575,
            "lat": 48.0449426,
            "lon": -1.602638,
            "tags": {
                "amenity": "charging_station",
                "authentication:none": "yes",
                "capacity": "2",
                "email": "sav@izivia.com",
                "fee": "yes",
                "motorcar": "yes",
                "network": "OUEST CHARGE - 35",
                "opening_hours": "24/7",
                "operator": "Sodetrel",
                "owner": "OUEST CHARGE - 35",
                "payment:credit_cards": "no",
                "phone": "+33972668001",
                "ref:EU:EVSE": "FR*S35*PSD3510811;FR*S35*PSD3510812",
                "socket:chademo": "2",
                "socket:type2": "2",
                "socket:type2_combo": "2",
                "source": "data.gouv.fr:Etalab - 05/2022",
            }, "type": "node", },
        {
            "country_code": "FR",
            "source_id": 2144376575,
            "operator": "Sodetrel",
            "data_source": DATA_SOURCE_KEY,
            "point": from_shape(Point(-1.602638, 48.0449426)),
            "date_created": datetime.now(), }, ), ])
def test_station_mapping(test_data, expected):
    station: Station = osm_mapper.map_station_osm(test_data, "FR")

    for attr, expected_val in expected.items():
        actual_val = getattr(station, attr)
        if attr == 'date_created':
            # Compare date_created with a tolerance of 30 seconds
            assert (abs(actual_val - expected_val)).total_seconds() < 60
        else:
            assert actual_val == expected_val


@pytest.mark.parametrize("input_data,expected", [(
        {  # Test case 1
            "id": 290918381,
            "lat": 48.5023337,
            "lon": 9.3760597,
            "tags": {
                "addr:city": "Bad Urach",
                "addr:country": "DE",
                "addr:housenumber": "2",
                "addr:postcode": "72574",
                "addr:street": "Bei den Thermen",
                "amenity": "charging_station",
                "beds": "144",
                "bicycle": "yes",
                "contact:email": "info@hotel-graf-eberhard.de",
                "contact:fax": "+49 7125 8214",
                "contact:phone": "+49 7125 1480",
                "contact:website": "https://www.hotel-graf-eberhard.de",
                "internet_access": "wlan",
                "internet_access:fee": "no",
                "name": "Hotel Graf Eberhard",
                "operator": "Hotel Graf Eberhard GmbH + Co. KG",
                "stars": "4",
                "tourism": "hotel",
                "wheelchair": "yes", }, "type": "node", },
            {
                "station_id": 1,
                "street": "Bei den Thermen 2",
                "town": "Bad Urach",
                "district": None,
                "state": None,
                "country": "DE",
                "gmaps_latitude": None,
                "gmaps_longitude": None,
            }, ), (
        {  # Test case 2
            "id": 7578833425,
            "lat": 49.9796288,
            "lon": 7.0870036,
            "tags": {
                "addr:city": "Kr\u00f6v",
                "addr:country": "DE",
                "addr:housenumber": "65",
                "addr:postcode": "54536",
                "addr:street": "Robert-Schuman-Stra\u00dfe",
                "amenity": "charging_station",
                "authentication:app": "yes",
                "capacity": "2",
                "description": "220 Volt",
                "fee": "yes",
                "opening_hours": "24/7",
                "parking:fee": "no",
                "ref:EU:EVSE": "DE*EON*E001342;DE*EON*E001343",
                "socket:type2": "2",
                "socket:type2:output": "22kW",
            }, "type": "node", },
            {
                "station_id": 1,
                "street": "Robert-Schuman-Straße 65",
                "town": "Kröv",
                "district": None,
                "state": None,
                "country": "DE",
                "gmaps_latitude": None,
                "gmaps_longitude": None}, ), ], )
def test_address_mapping(input_data, expected):
    address: Address = osm_mapper.map_address_osm(input_data, 1)

    for attr, expected_val in expected.items():
        assert getattr(address, attr) == expected_val


@pytest.mark.parametrize("input_data,expected", [(
        {  # Test case: missing address tags
            "id": 25397898,
            "lat": 49.0211913,
            "lon": 8.4310523,
            "tags": {
                "amenity": "charging_station",
                "amperage": "32",
                "authentication:membership_card": "yes",
                "bicycle": "no",
                "capacity": "2",
                "fee": "yes",
                "motorcar": "yes",
                "note": "maximale Leistung laut "
                "Aufschrift 22 kW, das m\u00fcsste "
                "Drehstrom mit 400 V und 32 A entsprechen",
                "old_ref": "1059",
                "opening_hours": "24/7",
                "operator": "EnBW AG",
                "payment:enbw_elektronautenkarte": "yes",
                "ref": "2039",
                "scooter": "no",
                "socket:type2": "2",
                "voltage": "400",
            }, "type": "node", },
        None)], )
def test_address_mapping__empty(input_data, expected):
    address = osm_mapper.map_address_osm(input_data, 1)

    assert address is expected


@pytest.mark.parametrize("json_data,station_id,expected", [(
        {  # Test case 1
            "id": 2420152562,
            "lat": 48.9277666,
            "lon": 8.4709132,
            "tags": {
                "amenity": "charging_station",
                "bicycle": "yes",
                "capacity": "4",
                "fee": "no",
                "socket:schuko": "4",
                "socket:type2:combo:output": "2.3 kW",
            }, "type": "node", },
        1,
        {
            "station_id": 1,
            "capacity": 4,
            "kw_list": [2.3],
            "total_kw": 2.3,
            "max_kw": 2.3,
            "ampere_list": None,
            "volt_list": None,
            "socket_type_list": ["Typ 2 Combo"],
            "dc_support": None,
        }, ), (
        {  # Test case 2
            "id": 2420152562,
            "lat": 48.9277666,
            "lon": 8.4709132,
            "tags": {
                "amenity": "charging_station",
                "capacity": "4",
                "socket:schuko": "4",
                "socket:schuko:output": "1.3 kva",
                "socket:type2:combo:output": "2.3 kW",
            }, "type": "node", },
        2,
        {
            "station_id": 2,
            "capacity": 4,
            "kw_list": [1.3, 2.3],
            "total_kw": 3.6,
            "max_kw": 2.3,
            "ampere_list": None,
            "volt_list": None,
            "socket_type_list": ["AC Schuko", "Typ 2 Combo"],
            "dc_support": None, }, ), ], )
def test_charging_mapping(json_data, station_id, expected):
    charging = osm_mapper.map_charging_osm(json_data, station_id)

    for attr, expected_val in expected.items():
        actual_val = getattr(charging, attr)
        if attr in ["total_kw", "max_kw"]:
            # Extra handling for float values comparison
            assert float_cmp_eq(actual_val, expected_val)
        else:
            assert actual_val == expected_val


@pytest.mark.parametrize("raw_amperage, expected", [
    ("", None),
    ("- ", None),
    (None, None),
    ("22 - 88", [22.0, 88.0]),
    ("16;32", [16.0, 32.0]),
    ("125 A - DC", [125.0]),
    ("22.6 kVA", [22.6]),
    ("1.2", [1.2]), ], )
def test_charging_mapping__amperage(raw_amperage, expected):
    charging = osm_mapper.map_charging_osm({
        "tags": {
            "amperage": raw_amperage, }, }, 1)

    assert charging.ampere_list == expected


@pytest.mark.parametrize(
    "amperage_tag_missing, expected", [({}, None)]  # Missing amperage key
)
def test_charging_mapping__amperage_missing_key(amperage_tag_missing, expected):
    charging = osm_mapper.map_charging_osm(
        {
            "tags": amperage_tag_missing,
        }, 1, )

    assert charging.ampere_list == expected


@pytest.mark.parametrize("raw_voltage, expected", [
    (None, None),
    ("400", [400]),
    ("400;230", [400, 230]),
    ("230;400", [230, 400]),
    ("1000", [1000]),
    ("22kW", None),
    ("400V", [400]),
    ("up to 22kW", None),
    ("400/500 V", [400, 500]),
    ("230 V AC", [230]),
    ("22.00", [22]),
    ("0-500", [500]),
    ("24,36", [24, 36]), ], )
def test_charging_mapping__voltage(raw_voltage, expected):
    json_data = {
        "tags": {
            "voltage": raw_voltage, }, }
    charging: Charging = osm_mapper.map_charging_osm(json_data, 1)

    assert charging.volt_list == expected


@pytest.mark.parametrize("test_input, expected", [
    (("socket:chademo", None, "1.0"), (["CHAdeMO"], [1.0])),
    (("socket:schuko", "2;yes", "2.22"), (["AC Schuko"], [2.22])),
    (("socket:tesla_supercharger", "Yes", "3.14"), (["Tesla Supercharger"], [3.14])),
    (("socket:type2", "2", "2.0"), (["AC Steckdose Typ 2"], [2.0])),
    (("socket:type2_combo", "55", "55.0"), (["Typ 2 Combo"], [55.0])),
    (("socket:type3", "3", "3.0"), (["AC Steckdose Typ 3"], [3.0])),
    (("socket:type3c", "2", "2.0"), (["AC Steckdose Typ 3c"], [2.0])),
    (("socket:typee", " 1 ", "1.0"), (["AC Steckdose Typ E"], [1.0])), ], )
def test_charging_mapping__socket_type_list(test_input, expected):
    ((socket_id, socket_id_val, socket_id_out_val), (expected_socket_lst, expected_kw_lst)) = (test_input, expected)
    json_data = {
        "tags": {
            socket_id: socket_id_val,
            f"{socket_id}:output": socket_id_out_val, }, }
    charging = osm_mapper.map_charging_osm(json_data, 1)

    assert charging.socket_type_list == expected_socket_lst
    assert charging.kw_list == expected_kw_lst


@pytest.mark.parametrize("kw_list,output_raw,expected", [
    ([10.0, 30.0], "50.0", 50.0),
    ([10.0, 20.0, 30.0], "40.0", 60.0),
    ([10.0, 20.0, 30.0], None, 60.0),
    ([], "40.0", 40.0),
    (None, "40.0", 40.0),
    (None, "50.0", 50.0),
    ([], None, 0.0),
    (None, [], 0.0),
    (None, None, 0.0), ], )
def test_calc_total_kw(kw_list, output_raw, expected):
    assert float_cmp_eq(osm_mapper.calc_total_kw(kw_list, output_raw), expected)


@pytest.mark.parametrize("input_str,expected", [
    ("", []),
    (None, []),
    ("  10kw,20kva, 30     ", [10.0, 20.0, 30.0]),
    ("test1, test2, test3", []), ], )
def test_extract_kw_list(input_str, expected):
    assert extract_kw_list(input_str) == expected


def test_extract_kw_map():
    datapoint = {
        "tags": {
            "TypeA:output": ["1", "2", "3"],
            "TypeB:output": ["4", "5", "6"],
            "TypeC:output": ["7", "8", "9"], }}

    expected_output = {
        "TypeA": [1.0, 2.0, 3.0],
        "TypeB": [4.0, 5.0, 6.0], }

    with patch(
        "charging_stations_pipelines.pipelines.osm.osm_mapper.SOCKET_TYPES",
        new_callable=PropertyMock,
        return_value={"TypeA": "valueA", "TypeB": "valueB"},
    ):
        with patch(
            "charging_stations_pipelines.pipelines.osm.osm_mapper.SOCKET_TYPES.keys",
            return_value=["TypeA", "TypeB"],
        ):
            with patch(
                "charging_stations_pipelines.pipelines.osm.osm_mapper.extract_kw_list",
                side_effect=lambda lst: list(map(float, lst)) if lst else None,
            ):
                assert extract_kw_map(datapoint) == expected_output
