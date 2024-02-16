import argparse
import json
import sys

import pandas

from charging_stations_pipelines import COUNTRIES


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="List country codes and other country related metadata available in eCharm",
        epilog="Example: python list-countries.py --json",
    )

    parser.add_argument(
        "-j",
        "--json",
        action="store_true",
        help="output country information in json format.",
    )

    return parser.parse_args(args)


command_line_args = parse_args(sys.argv[1:])
countries_json = json.dumps(COUNTRIES, default=vars, indent=4)
if command_line_args.json:
    print(countries_json)
else:
    df = pandas.read_json(json.dumps(COUNTRIES, default=vars, indent=4), orient="index")
    print(df.to_markdown(tablefmt="fancy_grid"))
