"""Unit tests for main.py"""

import pytest

from main import parse_args


def test_parse_valid_args():
    arguments = parse_args("import merge --countries de GB".split())
    assert arguments.tasks == ["import", "merge"]
    assert arguments.countries == ["DE", "GB"]
    assert not arguments.offline
    assert not arguments.delete_data


def test_parse_offline_arg():
    arguments = parse_args("import --offline".split())
    assert arguments.offline


def test_parse_delete_data_arg():
    arguments = parse_args("import --delete_data".split())
    assert arguments.delete_data


def test_parse_no_task_arg():
    with pytest.raises(SystemExit):
        parse_args([])


def test_parse_invalid_task_arg():
    # NOTE: in the pytest output, the error messages, like...:
    # ----
    # test/test_main.py::test_parse_invalid_task_arg usage: pytest [-h] [-c <country-code> [<country-code> ...]]
    #   [-v] [-o] [-d] [--export_file_descriptor <file descriptor>] [--export_format {csv,GeoJSON}] [--export_charging]
    #               [--export_merged_stations] [--export_all_countries] [--export_area <lon> <lat> <radius in m>]
    #               <task> [<task> ...]
    # pytest: error: argument <task>:
    #   invalid choice: 'invalid_task' (choose from 'import', 'merge', 'export', 'testdata')
    # ----
    # ... is an expected side-effect of using `pytest.raises(SystemExit)`
    with pytest.raises(SystemExit):
        parse_args("invalid_task --countries de".split())
