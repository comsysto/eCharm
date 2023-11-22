from unittest import TestCase


class Test(TestCase):

    def _extract_kw_socket_type(points) -> ([], []):
        # points = [{'evseId': 'AT*002*E200101*1',
        #            'energyInKw': 12,
        #            'authenticationModes': ['APP',
        #                                    'SMS',
        #                                    'WEBSITE'],
        #            'connectorTypes': ['CTESLA',
        #                               'S309-1P-16A',
        #                               'CG105',
        #                               'PAN'],
        #            'vehicleTypes': ['CAR', 'TRUCK', 'BICYCLE', 'MOTORCYCLE', 'BOAT']},
        #
        #           {'evseId': 'AT*002*E2001*5',
        #            'energyInKw': 15,
        #            'location': {'latitude': 48.198523499134545, 'longitude': 16.325340999197394},
        #            'priceInCentPerKwh': 12,
        #            'priceInCentPerMin': 13,
        #            'authenticationModes': [],
        #            'connectorTypes': ['CTESLA', 'CG105', 'CCCS2', 'CCCS1']}]

        # def udf(row: pl.Expr) -> pl.Expr:
        #     ekw = row.struct['energyInKw']
        #     ct = row.struct['connectorTypes']
        #     ret = {}
        #     for i, e in enumerate(ekw):
        #         for t in ct[i]:
        #             ret[t] = str(e)
        #     return ret

        # energyInKw=ff['points'].map_elements(lambda x: x.struct['energyInKw']),
        # connectorTypes=ff['points'].map_elements(lambda x: x.struct['connectorTypes']),

        # d = [{row['evseStationId']: row['kw_list']} for row in fff.iter_rows(named=True)]
        #
        # # Remove all keys with None values
        # dd = []
        # for e in d:
        #     dd.append({k: {kk: vv for kk, vv in v.items() if vv is not None} for k, v in e.items()})

        kw_list = []
        socket_type_list = []

        return kw_list, socket_type_list

    def test_points(self):
        points = [{'evseId': 'AT*002*E200101*1',
                   'energyInKw': 12,
                   'authenticationModes': ['APP',
                                           'SMS',
                                           'WEBSITE'],
                   'connectorTypes': ['CTESLA',
                                      'S309-1P-16A',
                                      'CG105',
                                      'PAN'],
                   'vehicleTypes': ['CAR', 'TRUCK', 'BICYCLE', 'MOTORCYCLE', 'BOAT']},

                  {'evseId': 'AT*002*E2001*5',
                   'energyInKw': 15,
                   'location': {'latitude': 48.198523499134545, 'longitude': 16.325340999197394},
                   'priceInCentPerKwh': 12,
                   'priceInCentPerMin': 13,
                   'authenticationModes': [],
                   'connectorTypes': ['CTESLA', 'CG105', 'CCCS2', 'CCCS1']}]

        kw_list, socket_type_list = self._extract_kw_socket_type(points)

        self.assertEqual(len(kw_list), len(socket_type_list))
        self.assertEqual([12.0, 12.0, 12.0, 12.0, 15.0, 15.0, 15.0, 15.0], kw_list)
        self.assertEqual(['CTESLA', 'S309-1P-16A', 'CG105', 'PAN',
                          'CTESLA', 'CG105', 'CCCS2', 'CCCS1'], socket_type_list)

    # def test_map_charging_ocm(self):
    #     json_data = {
    #         "NumberOfPoints": 2.0,
    #         "Connections": [
    #             {
    #                 "ID": 4492,
    #                 "ConnectionTypeID": 0,
    #                 "StatusTypeID": 0,
    #                 "LevelID": 2,
    #                 "Amps": 32,
    #                 "Voltage": 400,
    #                 "PowerKW": 12.8,
    #                 "Quantity": 1
    #             },
    #             {
    #                 "ID": 4493,
    #                 "ConnectionTypeID": 0,
    #                 "StatusTypeID": 0,
    #                 "LevelID": 2,
    #                 "Amps": 70,
    #                 "Voltage": 230,
    #                 "PowerKW": 16.1,
    #                 "Quantity": 1
    #             },
    #         ],
    #     }
    #     charging: Charging = map_charging_ocm(json_data, 1)
    #     self.assertEqual(charging.station_id, 1)
    #     self.assertEqual(charging.capacity, 2)
    #     self.assertEqual(charging.kw_list, None)
    #     self.assertEqual(charging.total_kw, 28.9)
    #     self.assertEqual(charging.max_kw, 16.1)
    #     self.assertEqual(charging.ampere_list, [32, 70])
    #     self.assertEqual(charging.volt_list, [400, 230])
    #     self.assertEqual(charging.socket_type_list, None)
    #     self.assertEqual(charging.dc_support, None)
