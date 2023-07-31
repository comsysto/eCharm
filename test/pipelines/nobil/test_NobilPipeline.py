from _decimal import Decimal
from unittest import TestCase

from charging_stations_pipelines.pipelines.nobil.NobilPipeline import parse_nobil_connectors, NobilConnector


class Test(TestCase):
    def test_parse_nobil_connectors(self):
        given = {
            "1": {
                "4": {
                    "attrtypeid": "4",
                    "attrname": "Connector",
                    "attrvalid": "32",
                    "trans": "Type 2",
                    "attrval": ""
                },
                "5": {
                    "attrtypeid": "5",
                    "attrname": "Charging capacity",
                    "attrvalid": "8",
                    "trans": "7,4 kW - 230V 1-phase max 32A",
                    "attrval": ""
                }
            },
            "2": {
                "4": {
                    "attrtypeid": "4",
                    "attrname": "Connector",
                    "attrvalid": "32",
                    "trans": "Type 2",
                    "attrval": ""
                },
                "5": {
                    "attrtypeid": "5",
                    "attrname": "Charging capacity",
                    "attrvalid": "8",
                    "trans": "75 kW DC",
                    "attrval": ""
                }
            }
        }

        actual_connectors: list[NobilConnector] = parse_nobil_connectors(given)
        self.assertEqual(2, len(actual_connectors))
        self.assertEqual(Decimal('7.4'), actual_connectors[0].power_in_kw)
        self.assertEqual(Decimal('75'), actual_connectors[1].power_in_kw)
