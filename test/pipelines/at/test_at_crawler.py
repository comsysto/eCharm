"""Test class for testing the functionality of the map_charging, map_station, and map_address functions."""

from unittest import TestCase

from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.at.econtrol_mapper import map_charging, map_station, map_address


class Test(TestCase):
    def test_map_charging(self):
        datapoint = {
            'points': [{'evseId': 'AT*002*E200101*1',
                        'energyInKw': 12,
                        'authenticationModes': ['APP', 'SMS', 'WEBSITE'],
                        'connectorTypes': ['CTESLA', 'S309-1P-16A', 'CG105', 'PAN'],
                        'vehicleTypes': ['CAR', 'TRUCK', 'BICYCLE', 'MOTORCYCLE', 'BOAT']},

                       {'evseId': 'AT*002*E2001*5',
                        'energyInKw': 15,
                        'location': {'latitude': 48.198523499134545, 'longitude': 16.325340999197394},
                        'priceInCentPerKwh': 12,
                        'priceInCentPerMin': 13,
                        'authenticationModes': ['SMS', "DEBIT_CARD", "CASH", "CREDIT_CARD"],
                        'connectorTypes': ['CTESLA', 'CG105', 'CCCS2', 'CCCS1']}]
        }

        c = map_charging(datapoint, 1)  # type: Charging

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

    def test_map_station(self):
        datapoint = {
            "city": "Reichenau im M\u00fchlkreis ",
            "contactName": "Marktgemeindeamt Reichenau i.M.",
            "description": "Ortsplatz vor dem Gemeindeamt",
            "email": "marktgemeindeamt@reichenau-ooe.at",
            "evseCountryId": "AT",
            "evseOperatorId": "000",
            "evseStationId": "EREI001",
            "freeParking": True,
            "greenEnergy": True,
            "label": "Marktplatz/Gemeindeamt",
            "location": {
                "latitude": 48.456161,
                "longitude": 14.349852
            },
            "openingHours": {
                "details": []
            },
            "points": [
                {
                    "authenticationModes": [],
                    "connectorTypes": [
                        "SCEE-7-8"
                    ],
                    "energyInKw": 3.0,
                    "evseId": "AT*000*EREI001",
                    "freeOfCharge": True,
                    "location": {
                        "latitude": 48.456161,
                        "longitude": 14.349852
                    },
                    "public": True,
                    "roaming": True,
                    "status": "UNKNOWN",
                    "vehicleTypes": [
                        "CAR",
                        "BICYCLE",
                        "MOTORCYCLE"
                    ]
                }
            ],
            "postCode": "4204",
            "public": True,
            "status": "ACTIVE",
            "street": "Marktplatz 2",
            "telephone": "+43 7211 82550",
            "website": "www.reichenau-ooe.at"
        }

        s = map_station(datapoint)  # type: Station

        # Column(String, index=True, nullable=True, unique=True)
        self.assertEqual('EREI001', s.source_id)  # evseStationId
        #  Column(String)
        self.assertEqual('AT_ECONTROL', s.data_source)
        # TODO check semantics
        # operator = Column(String)
        self.assertEqual('000', s.operator)  # evseOperatorId
        # TODO check semantics
        # Column(String)
        self.assertEqual(None, s.payment)
        # TODO check semantics
        # Column(String)
        self.assertEqual([], s.authentication)  # .points[] | .authenticationModes
        # Column(Geography(geometry_type='POINT', srid=4326))
        self.assertEqual(from_shape(Point(14.349852, 48.456161)), s.point)
        # Column(String)
        self.assertEqual(None, s.raw_data)
        # Column(String)
        self.assertEqual('AT', s.country_code)

    def test_map_address(self):
        datapoint = {
            "city": "Reichenau im M\u00fchlkreis ",
            "contactName": "Marktgemeindeamt Reichenau i.M.",
            "description": "Ortsplatz vor dem Gemeindeamt",
            "email": "marktgemeindeamt@reichenau-ooe.at",
            "evseCountryId": "AT",
            "evseOperatorId": "000",
            "evseStationId": "EREI001",
            "freeParking": True,
            "greenEnergy": True,
            "label": "Marktplatz/Gemeindeamt",
            "location": {
                "latitude": 48.456161,
                "longitude": 14.349852
            },
            "openingHours": {
                "details": []
            },
            "points": [
                {
                    "authenticationModes": [],
                    "connectorTypes": [
                        "SCEE-7-8"
                    ],
                    "energyInKw": 3.0,
                    "evseId": "AT*000*EREI001",
                    "freeOfCharge": True,
                    "location": {
                        "latitude": 48.456161,
                        "longitude": 14.349852
                    },
                    "public": True,
                    "roaming": True,
                    "status": "UNKNOWN",
                    "vehicleTypes": [
                        "CAR",
                        "BICYCLE",
                        "MOTORCYCLE"
                    ]
                }
            ],
            "postCode": "4204",
            "public": True,
            "status": "ACTIVE",
            "street": "Marktplatz 2",
            "telephone": "+43 7211 82550",
            "website": "www.reichenau-ooe.at"
        }

        a = map_address(datapoint, 3)  # type: Address

        # FK to station table
        self.assertEqual(3, a.station_id)
        # Column(Date)
        self.assertEqual(None, a.date_created)
        # Column(Date)
        self.assertEqual(None, a.date_updated)
        # Column(String)
        self.assertEqual('Marktplatz 2', a.street)
        # Column(String)
        self.assertEqual('Reichenau im Mühlkreis', a.town)
        # Column(String)
        self.assertEqual('4204', a.postcode)
        # Column(String)
        self.assertEqual(None, a.district_old)
        # Column(String)
        self.assertEqual(None, a.district)
        # Column(String)
        self.assertEqual(None, a.state_old)
        # Column(String)
        self.assertEqual(None, a.state)
        # Column(String)
        self.assertEqual('AT', a.country)
