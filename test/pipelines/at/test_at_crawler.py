from unittest import TestCase

from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.at.econtrol_mapper import map_charging, map_station


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

        self.assertEqual(c.station_id, 1)
        # TODO check capacity semantics
        self.assertEqual(8, c.capacity)
        self.assertEqual([12.0, 12.0, 12.0, 12.0, 15.0, 15.0, 15.0, 15.0], c.kw_list)
        self.assertEqual(108.0, c.total_kw)
        self.assertEqual(15.0, c.max_kw)
        self.assertEqual(None, c.ampere_list)
        self.assertEqual(None, c.volt_list)
        self.assertEqual(['CTESLA', 'S309-1P-16A', 'CG105', 'PAN', 'CTESLA', 'CG105', 'CCCS2', 'CCCS1'],
                         c.socket_type_list)
        self.assertEqual(None, c.dc_support)

    def test_map_station(self):
        # TODO get better data sample
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

        s = map_station(datapoint)  # type: Station

        # source_id = Column(String, index=True, nullable=True, unique=True)
        self.assertEqual(s.source_id, 'TODO')

        # data_source = Column(String)
        self.assertEqual('AT_ECONTROL', s.data_source)

        # operator = Column(String)
        # payment = Column(String)

        # TODO check semantics
        # authentication = Column(String)
        self.assertEqual(sorted(['APP', 'SMS', 'WEBSITE', "DEBIT_CARD", "CASH", "CREDIT_CARD"]), s.authentication)

        # point = Column(Geography(geometry_type='POINT', srid=4326))
        self.assertEqual('???', s.point)

        # raw_data = Column(String)
        # N/A

        # country_code = Column(String)
        self.assertEqual('AT', s.country_code)

    def test_map_address(self):
        # TODO get better data sample
        datapoint = {
        }
