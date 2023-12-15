"""Test the OCM mapper."""

import datetime

import pytest
from dateutil.tz import tzutc
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.pipelines.ocm.ocm_mapper import map_address_ocm, map_charging_ocm, map_station_ocm


# @pytest.mark.parametrize("row, country_code, expected", [
#     ({
#          "AddressInfo.Latitude": 51.5074,
#          "AddressInfo.Longitude": -0.1278,
#          "Title_y": "Test Station",
#          "ID": 123,
#          "DateCreated": "2023-01-01T00:00:00Z",
#          "DateUpdated": "2023-01-01T00:00:00Z"
#      }, 'GB',  {
#         "country_code": 'GB',
#         "source_id": 123,
#         "operator": "Test Station",
#         "data_source": 'OCM',
#         "point": from_shape(Point(-0.1278, 51.5074)),
#         "date_created": datetime.datetime(2023, 1, 1, 0, 0, tzinfo=tzutc()),
#         "date_updated": datetime.datetime(2023, 1, 1, 0, 0, tzinfo=tzutc())
#     }), ])
# def test_map_station_ocm(row, country_code, expected):
#     station = map_station_ocm(row, country_code)
#
#     for attribute, expected_value in expected.items():
#         assert getattr(station, attribute) ==  expected_value
#
#
# # FIXME
# @pytest.mark.parametrize("row,country_code,station_id,expected", [(
#         {"AddressInfo.AddressLine1": "Street 1",
#          "AddressInfo.Town": "Town1",
#          "AddressInfo.Postcode": "12345",
#          "AddressInfo.StateOrProvince": "Province1",
#          "Title_x": "Country1"
#         }, "US", 1,
#         # FIXME
#         "FIXME"),(
#         {"AddressInfo.AddressLine1": "Street 2",
#          "AddressInfo.Town": "Town2",
#          "AddressInfo.Postcode": "67890",
#          "AddressInfo.StateOrProvince": "Province2",
#          "Title_x": "Country2"}, "IN", 2,
#         # FIXME
#         "FIXME"),
# ])
# def test_map_address_ocm(row, country_code: str, station_id: int, expected):
#     # FIXME
#     result = map_address_ocm(row, country_code, station_id)
#     assert result == expected


@pytest.mark.parametrize("json_data,expected_charging", [
    ({
         "NumberOfPoints": 2.0,
         "Connections": [
             {
                 "ID": 4492,
                 "ConnectionTypeID": 0,
                 "StatusTypeID": 0,
                 "LevelID": 2,
                 "Amps": 32,
                 "Voltage": 400,
                 "PowerKW": 12.8,
                 "Quantity": 1,
             },
             {
                 "ID": 4493,
                 "ConnectionTypeID": 0,
                 "StatusTypeID": 0,
                 "LevelID": 2,
                 "Amps": 70,
                 "Voltage": 230,
                 "PowerKW": 16.1,
                 "Quantity": 1,
             }, ], },
     {
         "capacity": 2,
         "kw_list": None,
         "total_kw": 28.9,
         "max_kw": 16.1,
         "ampere_list": [32, 70],
         "volt_list": [400, 230],
         "socket_type_list": None,
         "dc_support": None}),
    ({
         'NumberOfPoints': 3,
         'Connections': [
             {'Title': 'Type A', 'Amps': 10, 'Voltage': 220, 'PowerKW': 2.2},
             {'Title': 'Type B', 'Amps': 15, 'Voltage': 220, 'PowerKW': 3.3},
             {'Title': 'Type C', 'Amps': 20, 'Voltage': 220, 'PowerKW': 4.4}]},{
         'capacity': 3,
         'kw_list': None,
         'ampere_list': [10, 15, 20],
         'volt_list': [220, 220, 220],
         'socket_type_list': 'Type A,Type B,Type C',
         'dc_support': None,
         'total_kw': 9.9,
         'max_kw': 4.4})])
def test_map_charging(json_data, expected_charging):
    charging = map_charging_ocm(json_data, 1)
    for attr, expected_value in expected_charging.items():
        assert getattr(charging, attr) == expected_value
