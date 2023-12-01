"""Test class for testing the functionality of the map_charging, map_station, and map_address functions."""

import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.pipelines.at.econtrol_mapper import map_address, map_charging, map_station


def test_map_station__plain():
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

    s = map_station(datapoint)

    # source_id == evseCountryId * evseOperatorId * evseStationId
    assert s.source_id == 'AT*000*EREI001'  # Column(String, index=True, nullable=True, unique=True)
    assert s.data_source == 'AT_ECONTROL'  # Column(String)
    # TODO check semantics
    assert s.operator == '000'  # Column(String) / evseOperatorId
    # TODO check semantics
    self.assertEqual(None, s.payment)  # Column(String)
    # TODO check semantics
    self.assertEqual(None, s.authentication)  # Column(String) / .points[] | .authenticationModes
    self.assertEqual(from_shape(Point(14.349852, 48.456161)),
                     s.point)  # Column(Geography(geometry_type='POINT', srid=4326))
    self.assertEqual(datapoint.to_json(), s.raw_data)  # Note: Column(JSON)
    self.assertEqual('AT', s.country_code)  # Column(String)


def test_map_address__plain():
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

    self.assertEqual(3, a.station_id)  # FK to station table
    self.assertEqual(None, a.date_created)  # Column(Date)
    self.assertEqual(None, a.date_updated)  # Column(Date)
    self.assertEqual('Marktplatz 2', a.street)  # Column(String)
    self.assertEqual('Reichenau im Mühlkreis', a.town)  # Column(String)
    self.assertEqual('4204', a.postcode)  # Column(String)
    self.assertEqual(None, a.district_old)  # Column(String)
    self.assertEqual(None, a.district)  # Column(String)
    self.assertEqual(None, a.state_old)  # Column(String)
    self.assertEqual(None, a.state)  # Column(String)
    self.assertEqual('AT', a.country)  # Column(String)


def test_map_charging__plain():
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
    self.assertEqual(2, c.capacity)
    self.assertEqual([12.0, 12.0, 12.0, 12.0, 15.0, 15.0, 15.0, 15.0], c.kw_list)
    self.assertEqual(108.0, c.total_kw)
    self.assertEqual(15.0, c.max_kw)
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
                    'connectorTypes': raw[1]  # ['CTESLA', 'S309-1P-16A', 'CG105', 'PAN'],
                },
                # {
                #     'energyInKw': 15,
                #     'connectorTypes': ['CTESLA', 'CG105']}
            ]
        })

        exp_kw_list, exp_max_kw, exp_total_kw = expected

        c = map_charging(raw_datapoint, 1)

        assert c.kw_list == exp_kw_list
        assert c.max_kw == exp_max_kw
        assert c.total_kw == exp_total_kw
