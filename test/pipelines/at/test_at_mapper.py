"""Test class for testing the functionality of the map_charging, map_station, and map_address functions."""

from unittest import TestCase

import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.pipelines.at.econtrol_mapper import map_address, map_charging, map_station


class Test(TestCase):

    def test_map_station__plain(self):
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
        self.assertEqual('AT*000*EREI001', s.source_id)  # Column(String, index=True, nullable=True, unique=True)
        self.assertEqual('AT_ECONTROL', s.data_source)  # Column(String)
        # TODO check semantics
        self.assertEqual('000', s.operator)  # Column(String) / evseOperatorId
        # TODO check semantics
        self.assertEqual(None, s.payment)  # Column(String)
        # TODO check semantics
        self.assertEqual(None, s.authentication)  # Column(String) / .points[] | .authenticationModes
        self.assertEqual(from_shape(Point(14.349852, 48.456161)),
                         s.point)  # Column(Geography(geometry_type='POINT', srid=4326))
        self.assertEqual(datapoint.to_json(), s.raw_data)  # Note: Column(JSON)
        self.assertEqual('AT', s.country_code)  # Column(String)

    def test_map_address__plain(self):
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
        self.assertEqual(None, a.district)  # Column(String)
        self.assertEqual(None, a.state)  # Column(String)
        self.assertEqual('AT', a.country)  # Column(String)

    def test_map_charging__plain(self):
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

        self.assertEqual(1, c.station_id)
        self.assertEqual(2, c.capacity)
        self.assertEqual([12.0, 12.0, 12.0, 12.0, 15.0, 15.0, 15.0, 15.0], c.kw_list)
        self.assertEqual(108.0, c.total_kw)
        self.assertEqual(15.0, c.max_kw)
        self.assertEqual(None, c.ampere_list)
        self.assertEqual(None, c.volt_list)
        self.assertEqual(['CTESLA', 'S309-1P-16A', 'CG105', 'PAN', 'CTESLA', 'CG105', 'CCCS2', 'CCCS1'],
                         c.socket_type_list)
        self.assertEqual(None, c.dc_support)

    def test_map_charging__kw_list(self):
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

            self.assertEqual(exp_kw_list, c.kw_list)
            self.assertEqual(exp_max_kw, c.max_kw)
            self.assertEqual(exp_total_kw, c.total_kw)
