"""Tests for the NobilPipeline."""

from _decimal import Decimal

from charging_stations_pipelines.pipelines.nobil.nobil_pipeline import (
    parse_nobil_connectors,
)


def test_parse_nobil_connectors():
    given = {
        "1": {
            "4": {
                "attrtypeid": "4",
                "attrname": "Connector",
                "attrvalid": "32",
                "trans": "Type 2",
                "attrval": "",
            },
            "5": {
                "attrtypeid": "5",
                "attrname": "Charging capacity",
                "attrvalid": "8",
                "trans": "7,4 kW - 230V 1-phase max 32A",
                "attrval": "",
            },
        },
        "2": {
            "4": {
                "attrtypeid": "4",
                "attrname": "Connector",
                "attrvalid": "32",
                "trans": "Type 2",
                "attrval": "",
            },
            "5": {
                "attrtypeid": "5",
                "attrname": "Charging capacity",
                "attrvalid": "8",
                "trans": "75 kW DC",
                "attrval": "",
            },
        },
    }

    actual_connectors = parse_nobil_connectors(given)

    assert len(actual_connectors) == 2
    assert actual_connectors[0].power_in_kw == Decimal("7.4")
    assert actual_connectors[1].power_in_kw == Decimal("75")
