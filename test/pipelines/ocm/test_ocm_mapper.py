"""Test the OCM mapper."""

from charging_stations_pipelines.pipelines.ocm.ocm_mapper import map_charging_ocm
from test.shared import is_float_eq


def test_map_charging_ocm():
    json_data = {
        "NumberOfPoints": 2.0,
        "Connections":    [
            {
                "ID":               4492,
                "ConnectionTypeID": 0,
                "StatusTypeID":     0,
                "LevelID":          2,
                "Amps":             32,
                "Voltage":          400,
                "PowerKW":          12.8,
                "Quantity":         1
            },
            {
                "ID":               4493,
                "ConnectionTypeID": 0,
                "StatusTypeID":     0,
                "LevelID":          2,
                "Amps":             70,
                "Voltage":          230,
                "PowerKW":          16.1,
                "Quantity":         1
            },
        ],
    }

    charging = map_charging_ocm(json_data, 1)

    assert charging.station_id == 1
    assert charging.capacity == 2
    assert charging.kw_list is None
    assert is_float_eq(charging.total_kw, 28.9)
    assert is_float_eq(charging.max_kw, 16.1)
    assert charging.ampere_list == [32, 70]
    assert charging.volt_list == [400, 230]
    assert charging.socket_type_list is None
    assert charging.dc_support is None
