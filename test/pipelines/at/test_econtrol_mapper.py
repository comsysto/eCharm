"""Test class for testing the functionality of the map_charging, map_station, and map_address functions."""

import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.pipelines.at.econtrol_mapper import map_address, map_charging, map_station
from charging_stations_pipelines.shared import float_cmp_eq


def test_map_station():
    # noinspection DuplicatedCode
    datapoint = pd.Series({
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
    })

    s = map_station(datapoint, 'AT')

    assert s.source_id == 'AT*000*EREI001'
    assert s.data_source == 'AT_ECONTROL'
    assert s.evse_country_id == 'AT'
    assert s.evse_operator_id == '000'
    assert s.evse_station_id == 'EREI001'
    assert s.operator == 'Marktgemeindeamt Reichenau i.M.'
    assert s.payment is None
    assert s.authentication is None
    assert s.point == from_shape(Point(14.349852, 48.456161))
    assert s.raw_data == datapoint.to_json()
    assert s.country_code == 'AT'


def test_map_address():
    # noinspection DuplicatedCode
    datapoint = pd.Series({
        "city":           "Reichenau im M\u00fchlkreis ",
        "contactName":    "Marktgemeindeamt Reichenau i.M.",
        "description":    "Ortsplatz vor dem Gemeindeamt",
        "email":          "marktgemeindeamt@reichenau-ooe.at",
        "evseCountryId":  "AT",
        "evseOperatorId": "000",
        "evseStationId":  "EREI001",
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
    })

    a = map_address(datapoint, 3)

    assert a.station_id == 3
    assert a.date_created is None
    assert a.date_updated is None
    assert a.street == 'Marktplatz 2'
    assert a.town == 'Reichenau im Mühlkreis'
    assert a.postcode == '4204'
    assert a.district is None
    assert a.state is None
    assert a.country == 'AT'


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
