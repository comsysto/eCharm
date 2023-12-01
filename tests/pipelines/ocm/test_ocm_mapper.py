from unittest import TestCase

from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.pipelines.ocm.ocm_mapper import map_charging_ocm


class Test(TestCase):
    def test_map_charging_ocm(self):
        json_data = {
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
                    "Quantity": 1
                },
                {
                    "ID": 4493,
                    "ConnectionTypeID": 0,
                    "StatusTypeID": 0,
                    "LevelID": 2,
                    "Amps": 70,
                    "Voltage": 230,
                    "PowerKW": 16.1,
                    "Quantity": 1
                },
            ],
        }
        charging: Charging = map_charging_ocm(json_data, 1)
        self.assertEqual(charging.station_id, 1)
        self.assertEqual(charging.capacity, 2)
        self.assertEqual(charging.kw_list, None)
        self.assertEqual(charging.total_kw, 28.9)
        self.assertEqual(charging.max_kw, 16.1)
        self.assertEqual(charging.ampere_list, [32, 70])
        self.assertEqual(charging.volt_list, [400, 230])
        self.assertEqual(charging.socket_type_list, None)
        self.assertEqual(charging.dc_support, None)
