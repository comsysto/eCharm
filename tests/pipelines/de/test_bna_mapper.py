from charging_stations_pipelines.models.address import Address
from charging_stations_pipelines.models.charging import Charging
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines.pipelines.de.bna_mapper import _clean_attributes, lat_long_hash, map_address_bna, \
    map_charging_bna, map_station_bna

# TODO check leading/trailing whitespaces in socket types of BNA and OCM data
# 23750	713596	POINT (6.769936467 51.280628314)	BNA,OCM	SWD AG	Frachtstraße 1	Düsseldorf	2.0	['AC Steckdose Typ 2', ' AC Schuko', 'AC Steckdose Typ 2', ' AC Schuko']	False	44.0	22.0	51.280628314	6.769936467

# TODO pytest tests are not detected on the CLI

# TODO fix: "python -m unittest: error: the following arguments are required: <task>"
# [2023-11-30 16:17:13 +0100] INFO     charging_stations_pipelines.pipelines.de.bna_crawler: Downloaded file size: 8195041 bytes
# .....usage: python -m unittest [-h] [-c <country-code> [<country-code> ...]] [-v] [-o] [-d] [--export_file_descriptor <file descriptor>]
#                           [--export_format {csv,GeoJSON}] [--export_charging] [--export_merged_stations] [--export_all_countries]
#                           [--export_area <lon> <lat> <radius in m>]
#                           <task> [<task> ...]
# python -m unittest: error: argument <task>: invalid choice: 'invalid_task' (choose from 'import', 'merge', 'export', 'testdata')
# .usage: python -m unittest [-h] [-c <country-code> [<country-code> ...]] [-v] [-o] [-d] [--export_file_descriptor <file descriptor>]
#                           [--export_format {csv,GeoJSON}] [--export_charging] [--export_merged_stations] [--export_all_countries]
#                           [--export_area <lon> <lat> <radius in m>]
#                           <task> [<task> ...]
# python -m unittest: error: the following arguments are required: <task>
#

def test_lat_long_hash_creates_unique_hash():
    hash1 = lat_long_hash(52.5200, 13.4050, "BNA")
    hash2 = lat_long_hash(52.5200, 13.4050, "BNA")
    assert hash1 == hash2


def test_clean_attributes_resets_capacity_above_max():
    charging = Charging()
    charging.capacity = 5
    charging = _clean_attributes(charging)
    assert charging.capacity == 2


def test_map_station_bna_creates_station_with_correct_attributes():
    row = {"Breitengrad":         "52.5200",
           "Längengrad":          "13.4050",
           "Betreiber":           "Test Operator",
           "Inbetriebnahmedatum": "2022-01-01"}
    station = map_station_bna(row)
    assert isinstance(station, Station)
    assert station.operator == "Test Operator"


def test_map_address_bna_creates_address_with_correct_attributes():
    row = {"Straße":                 "Test Street",
           "Hausnummer":             "123",
           "Postleitzahl":           "10115",
           "Ort":                    "Berlin",
           "Kreis/kreisfreie Stadt": "Berlin",
           "Bundesland":             "Berlin"}
    address = map_address_bna(row, "test_id")
    assert isinstance(address, Address)
    assert address.street == "Test Street 123"


def test_map_charging_bna_creates_charging_with_correct_attributes():
    row = {"Nennleistung Ladeeinrichtung [kW]": "50",
           "Anzahl Ladepunkte":                 "2",
           "Steckertypen":                      "Type2, CCS"}
    charging = map_charging_bna(row, "test_id")
    assert isinstance(charging, Charging)
    assert charging.capacity == 2


def test_map_charging_bna_handles_missing_kw():
    row = {"Nennleistung Ladeeinrichtung [kW]": None,
           "Anzahl Ladepunkte":                 "2",
           "Steckertypen":                      "Type2, CCS"}
    charging = map_charging_bna(row, "test_id")
    assert charging.total_kw is None


def test_map_charging_bna_handles_invalid_kw():
    row = {"Nennleistung Ladeeinrichtung [k W]": "invalid",
           "Anzahl Ladepunkte":                 "2",
           "Steckertypen":                      "Type2, CCS"}
    charging = map_charging_bna(row, "test_id")
    assert charging.total_kw is None
