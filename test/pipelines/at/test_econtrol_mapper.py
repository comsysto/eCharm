"""Test class for testing the functionality of the map_charging, map_station, and map_address functions."""

import pandas as pd
import pytest
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.pipelines.at.econtrol_mapper import aggregate_attribute, map_charging, map_station
from charging_stations_pipelines.shared import float_cmp_eq


@pytest.mark.parametrize("datapoint, expected_result", [(pd.Series({
        "city":           "Reichenau im M\u00fchlkreis ",
        "contactName":    "Marktgemeindeamt Reichenau i.M.",
        "description":    "Ortsplatz vor dem Gemeindeamt",
        "email":          "marktgemeindeamt@reichenau-ooe.at",
        "evseCountryId":  " AT ",
        "evseOperatorId": " 000",
        "evseStationId":  "     EREI001 ",
        "freeParking":    True,
        "greenEnergy":    True,
        "label":          "Marktplatz/Gemeindeamt",
        "location":       {
            "latitude":  48.456161,
            "longitude": 14.349852
        },
        "openingHours":   {
            "details": []
        },
        "points":         [
            {
                "authenticationModes": [],
                "connectorTypes":      ["SCEE-7-8"],
                "energyInKw":          3.0,
                "evseId":              "AT*000*EREI001",
                "freeOfCharge":        True,
                "location":            {
                    "latitude":  48.456161,
                    "longitude": 14.349852
                },
                "public":              True,
                "roaming":             True,
                "status":              "UNKNOWN",
                "vehicleTypes":        [
                    "CAR",
                    "BICYCLE",
                    "MOTORCYCLE"
                ]
            }
        ],
        "postCode":       "4204",
        "public":         True,
        "status":         "ACTIVE",
        "street":         "Marktplatz 2",
        "telephone":      "+43 7211 82550",
        "website":        "www.reichenau-ooe.at"
    }), {
        'source_id': 'AT*000*EREI001',
        'data_source': 'AT_ECONTROL',
        'evse_country_id': 'AT',
        'evse_operator_id': '000',
        'evse_station_id': 'EREI001',
        'operator': 'Marktgemeindeamt Reichenau i.M.',
        'payment': None,
        'authentication': None,
        'point': from_shape(Point(14.349852, 48.456161)),
        'country_code': 'AT'
    }), (pd.Series({
        "city":           "Reichenau im M\u00fchlkreis ",
        "description":    "Ortsplatz vor dem Gemeindeamt",
        "email":          "marktgemeindeamt@reichenau-ooe.at",
        "evseCountryId":  " DE",
        "evseOperatorId": " 000",
        "evseStationId":  "     EREI001 ",
        "freeParking":    True,
        "greenEnergy":    True,
        "label":          "Marktplatz/Gemeindeamt",
        "location":       {
            "latitude":  48.111,
            "longitude": 14.222
        },
    }), {
        'source_id': 'AT*000*EREI001',
        'data_source': 'AT_ECONTROL',
        'evse_country_id': 'DE',
        'evse_operator_id': '000',
        'evse_station_id': 'EREI001',
        'operator': None,
        'payment': None,
        'authentication': None,
        'point': from_shape(Point(14.222, 48.111)),
        'country_code': 'AT'
    })])
def test_map_station(datapoint, expected_result):
    s = map_station(datapoint, 'AT')
    # noinspection DuplicatedCode
    assert s.source_id == expected_result['source_id']
    assert s.data_source == expected_result['data_source']
    assert s.evse_country_id == expected_result['evse_country_id']
    assert s.evse_operator_id == expected_result['evse_operator_id']
    assert s.evse_station_id == expected_result['evse_station_id']
    assert s.operator == expected_result['operator']
    assert s.payment is expected_result['payment']
    assert s.authentication is expected_result['authentication']
    assert s.point == expected_result['point']
    assert s.raw_data == datapoint.to_json()
    assert s.country_code == expected_result['country_code']


def test_map_charging():
    datapoint = pd.Series({
        'points': [{'evseId':              'AT*002*E200101*1',
                    'energyInKw':          12,
                    'authenticationModes': ['APP', 'SMS', 'WEBSITE'],
                    'connectorTypes':      ['CTESLA', 'S309-1P-16A', 'CG105', 'PAN'],
                    'vehicleTypes':        ['CAR', 'TRUCK', 'BICYCLE', 'MOTORCYCLE', 'BOAT']},
                   {'evseId':              'AT*002*E2001*5',
                    'energyInKw':          15,
                    'location':            {'latitude': 48.198523499134545, 'longitude': 16.325340999197394},
                    'priceInCentPerKwh':   12,
                    'priceInCentPerMin':   13,
                    'authenticationModes': ['SMS', "DEBIT_CARD", "CASH", "CREDIT_CARD"],
                    'connectorTypes':      ['CTESLA', 'CG105', 'CCCS2', 'CCCS1']}]
    })

    c = map_charging(datapoint, 1)

    assert c.station_id == 1
    assert c.capacity == 2
    assert c.kw_list == [12.0, 12.0, 12.0, 12.0, 15.0, 15.0, 15.0, 15.0]
    assert float_cmp_eq(c.total_kw, 108.0)
    assert float_cmp_eq(c.max_kw, 15.0)
    assert c.ampere_list is None
    assert c.volt_list is None
    assert c.socket_type_list == ['CTESLA', 'S309-1P-16A', 'CG105', 'PAN', 'CTESLA', 'CG105', 'CCCS2', 'CCCS1']
    assert c.dc_support is None


def test_map_charging__kw_list():
    sample_data = [
        ([None, None], [None, None, None]),
        ([None, []], [None, None, None]),
        ([3.14, ['CTESLA', 'S309-1P-16A']], [[3.14, 3.14], 3.14, 3.14 + 3.14]),
    ]

    for raw, expected in sample_data:
        raw_datapoint = pd.Series({
            'points': [
                {
                    'energyInKw':     raw[0],
                    'connectorTypes': raw[1]
                }
            ]
        })

        exp_kw_list, exp_max_kw, exp_total_kw = expected

        c = map_charging(raw_datapoint, 1)

        assert c.kw_list == exp_kw_list
        assert c.max_kw == exp_max_kw
        assert c.total_kw == exp_total_kw


@pytest.mark.parametrize("points, attr, expected_result", [
    (pd.Series([
        {'test_attr': ['a', 'b', 'c'], },
        {'test_attr': ['c', 'd', 'e'], },
        {'test_attr': [], }, ]),
     'test_attr',
     [['a', 'b', 'c'], ['c', 'd', 'e'], []]),
    (pd.Series([{'test_attr': ['a', 'b', 'c']}]), 'test_attr', [['a', 'b', 'c']]),
    (pd.Series([{'test_attr': ['a', 'b', 'c']}, {'test_attr': ['d', 'e', 'f']}]), 'test_attr',
     [['a', 'b', 'c'], ['d', 'e', 'f']]),
    (pd.Series([], dtype='float64'), 'test_attr', []),
    (None, None, None),
    (None, 'test_attr', None),
    (pd.Series([{'test_attr': ['a', 'b', 'c']}]), None, [[]]),
    (pd.Series([{'test_attr': ['a', 'b']}]), 'missing_attr', [[]]), ])
def test_aggregate_attribute(points, attr, expected_result):
    result = aggregate_attribute(points, attr, )
    assert result == expected_result
