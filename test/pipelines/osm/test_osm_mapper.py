from unittest import TestCase

from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.pipelines.osm.osm_mapper import map_charging_osm, map_address_osm


class Test(TestCase):
    def test_address_mapping(self):
        json_data = {
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
                "contact:website": "http://www.hotel-graf-eberhard.de",
                "internet_access": "wlan",
                "internet_access:fee": "no",
                "name": "Hotel Graf Eberhard",
                "operator": "Hotel Graf Eberhard GmbH + Co. KG",
                "stars": "4",
                "tourism": "hotel",
                "wheelchair": "yes"
            },
            "type": "node"
        }

        address: Address = map_address_osm(json_data, 1)
        print(address)

        self.assertEqual(1, address.station_id)
        self.assertEqual('Bei den Thermen 2', address.street)
        self.assertEqual('Bad Urach', address.town)
        self.assertEqual(None, address.district_old)
        self.assertEqual(None, address.district)
        self.assertEqual(None, address.state)
        self.assertEqual('DE', address.country)
        self.assertEqual(None, address.gmaps_latitude)
        self.assertEqual(None, address.gmaps_longitude)

    def test_charging_mapping__plain_01(self):
        json_data = {
            "id": 2420152562,
            "lat": 48.9277666,
            "lon": 8.4709132,
            "tags": {
                "amenity": "charging_station",
                "bicycle": "yes",
                "capacity": "4",
                "fee": "no",
                "socket:schuko": "4",
                "socket:type2:combo:output": "2.3 kW"
            },
            "type": "node"
        }

        charging: Charging = map_charging_osm(json_data, 1)
        print(charging)

        self.assertEqual(1, charging.station_id)
        self.assertEqual(4, charging.capacity)
        self.assertEqual([2.3], charging.kw_list)
        self.assertEqual(2.3, charging.total_kw)
        self.assertEqual(2.3, charging.max_kw)
        self.assertEqual([], charging.ampere_list)
        self.assertEqual([], charging.volt_list)
        self.assertEqual(['AC Schuko'], charging.socket_type_list)
        self.assertEqual(False, charging.dc_support)

    def test_charging_mapping__amperage_01(self):
        json_data = {
            "tags": {
                "amperage": "1.2",
            },
        }
        charging: Charging = map_charging_osm(json_data, 1)
        self.assertEqual([1.2], charging.ampere_list)

    def test_charging_mapping__amperage_02(self):
        json_data = {
            "tags": {
                "amperage": "22.6 kVA",
            },
        }
        charging: Charging = map_charging_osm(json_data, 1)
        self.assertEqual([22.6], charging.ampere_list)

    def test_charging_mapping__amperage_03(self):
        json_data = {
            "tags": {
                "amperage": "125 A - DC",
            },
        }
        charging: Charging = map_charging_osm(json_data, 1)
        self.assertEqual([125.0], charging.ampere_list)

    def test_charging_mapping__amperage_04(self):
        json_data = {
            "tags": {
`                "amperage": "16;32",
            },
        }
        charging: Charging = map_charging_osm(json_data, 1)
        self.assertEqual([16, 32], charging.ampere_list)

    def test_charging_mapping__amperage_05(self):
        json_data = {
            "tags": {
                "amperage": "22 - 88",
            },
        }
        charging: Charging = map_charging_osm(json_data, 1)
        self.assertEqual([22, 88], charging.ampere_list)

    def test_charging_mapping__amperage_06(self):
        json_data = {
            "tags": {
                "amperage": None,
            },
        }
        charging: Charging = map_charging_osm(json_data, 1)
        self.assertEqual([], charging.ampere_list)

    def test_charging_mapping__amperage_07(self):
        json_data = {
            "tags": {
            },
        }
        charging: Charging = map_charging_osm(json_data, 1)
        self.assertEqual([], charging.ampere_list)

    def test_charging_mapping__amperage_08(self):
        json_data = {
            "tags": {
                "amperage": "",
            },
        }
        charging: Charging = map_charging_osm(json_data, 1)
        self.assertEqual([], charging.ampere_list)

    def test_charging_mapping__amperage_09(self):
        json_data = {
            "tags": {
                "amperage": "- ",
            },
        }
        charging: Charging = map_charging_osm(json_data, 1)
        self.assertEqual([], charging.ampere_list)

    def test_charging_mapping__voltage(self):
        # +-------------------------------------+-----+
        # |voltage                              |count|
        # +-------------------------------------+-----+
        # |null                                 |53167|
        # |"400"                                |1499 |
        # |"400;230"                            |83   |
        # |"230;400"                            |56   |
        # |"1000"                               |38   |
        # |"22kW"                               |11   |
        # |"up to 22kW"                         |10   |
        # |"400/500 V"                          |1    |
        # |"230 V AC"                           |1    |
        # |"22.00"                              |1    |
        # |"0-500"                              |1    |
        # |"24,36"                              |1    |
        # +-------------------------------------+-----+
        sample_data = [
            (None, []),
            ("400", [400]),
            ("400;230", [400, 230]),
            ("230;400", [230, 400]),
            ("1000", [1000]),
            ("22kW", []),
            ("400V", [400]),
            ("up to 22kW", []),
            ("400/500 V", [400, 500]),
            ("230 V AC", [230]),
            ("22.00", [22]),
            ("0-500", [500]),
            ("24,36", [24, 36]), ]
        for raw, expected in sample_data:
            json_data = {
                "tags": {
                    "voltage": raw,
                },
            }
            charging: Charging = map_charging_osm(json_data, 1)
            self.assertEqual(expected, charging.volt_list)

    def test_charging_mapping__socket_type_list(self):
        # +-----------------------------+-----+
        # |socket_type2                 |count|
        # +-----------------------------+-----+
        # |null                         |32137|
        # |"2"                          |12915|
        # |"yes"                        |3166 |
        # |"22 kW"                      |6    |
        # |"60"                         |6    |
        # |"output=22kw"                |3    |
        # |"22kw"                       |2    |
        # |"2-3A"                       |1    |
        # |"2;yes"                      |1    |
        # |"IEC 62196 Type 2a"          |1    |
        # |"Yes"                        |1    |
        # +-----------------------------+-----+
        sample_data = [  # list[tuple[tuple[str, str], list[str]]]
            (('socket:chademo', None), []),
            (('socket:schuko', '2;yes'), ['AC Schuko']),
            (('socket:tesla_supercharger', 'Yes'), ['Tesla Supercharger']),
            (('socket:type2', '2'), ['AC Steckdose Typ 2']),
            (('socket:type2_combo', '55'), ['AC Steckdose Typ 2 Combo']),
            (('socket:type3', '3'), ['AC Steckdose Typ 3']),
            (('socket:type3c', '2'), ['AC Steckdose Typ 3c']),
            (('socket:typee', ' 1 '), ['AC Steckdose Typ E']), ]
        for ((socket_type, raw), expected) in sample_data:  # type: tuple[tuple[str, str], list[str]]
            json_data = {
                "tags": {
                    socket_type: raw,
                },
            }
            charging: Charging = map_charging_osm(json_data, 1)
            self.assertEqual(expected, charging.socket_type_list)
