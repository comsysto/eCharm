# eCharm: Electric Vehicle Charging Map

## Table of contents

* [General info](#general-info)
* [Setup](#setup)
* [Contributing](#contributing)

## General info

`eCharm` stands for Electric Vehicle Charging Map and can best be described as an electronic vehicle charging stations
data integrator.
It downloads data from different publicly available sources, converts it into a common format,
searches for duplicates in the different sources and merges the data (e.g. the attributes).
All steps are decoupled, so it's easy to integrate your own data source.

### Data Sources

The primary data sources for electronic vehicle charging stations that are generally available across Europe are
Open Street Map (OSM) and Open Charge Map (OCM).

For some countries, we are able to then integrate the official government data source.
Just to name a few examples, the government data source we use for Germany is Bundesnetzagentur (BNA),
or the National Chargepoint Registry (NCR) for the UK.

### Technologies

We use (among others) the following technologies. Some basic familiarity is needed to set up and run the scripts.

* Python 3.9
* Docker

If you want to contribute or change details of the code for yourself, some familiarity is needed with

* Data and computing packages like pandas
* Alembic
* Sqlalchemy
* Postgres/PostGis

## Setup

### Configure a python virtual environment (skip if your IDE does that for you)

#### Install Pyenv if not already existing

Linux:

    curl https://pyenv.run | bash

Mac:

    brew install pyenv

Install and activate Python 3.9.x , e.g.

    pyenv install 3.9.18
    pyenv local 3.9.18

Check local version

    python --version

#### Create environment and activate:

```
python -m venv venv
source venv/bin/activate  
```

#### Install the requirements file:

```
pip install -r requirements.txt  
```

##### Trouble Shooting

If pip install is failing during psycopg2 install:

1. Make sure you have the following packages installed on your system
    - Ubuntu users: `build-essential`, `libgdal-dev`,`libpq5`
    - Mac users: A postgres version needs to be installed, e.g. via `brew install postgresql`
2. If there are still errors, search for openssl lib path (`which openssl` may help)
   and identify which one is needed for linking the compiler for psycopg2 correctly.
   Then export it as the corresponding LDFLAGS environment variable before pip install, for instance,
   `export LDFLAGS="-L/usr/local/Cellar/openssl@3/3.1.3/lib"`

#### Set environment variables

The following configuration parameters need to be set when using eCharm:

* DB_NAME: Specifies the name of the database to connect to.
* DB_HOST: Specifies the hostname or IP address of the database server.
* DB_PORT: Specifies the port number on which the database server is listening.
* DB_USER: Specifies the username to use when authenticating with the database server.
* DB_PASSWORD: Specifies the password to use when authenticating with the database server.
* DB_SCHEMA: Optionally possible to define schema where tables should be created. If not set it defaults to public.
* DB_TABLE_PREFIX: Optionally possible to define prefix for table names (e.g. `echarm_` would result in a
  table `echarm_stations`)
* DB_ALEMBIC_RESTRICT_TABLES: Optionally possible to set to `true` (default `false`), i.e. it only considers eCharm
  tables when checking for DB schema changes
* NOBIL_APIKEY: Specifies the API key required for accessing the NOBIL API. The NOBIL API is used to retrieve data from
  Sweden and Norway

##### Example

Put the following content to `.env` file in the root directory:

    DB_NAME=gis
    DB_HOST=localhost
    DB_PORT=54322
    DB_USER=docker
    DB_PASSWORD=docker
    DB_SCHEMA=friendly_fox

    # optional for DB:
    DB_TABLE_PREFIX=echarm_
    DB_ALEMBIC_RESTRICT_TABLES=true
    # for Nobil access:
    NOBIL_APIKEY=<MY_NOBIL_API_KEY>
    # for e-control.at / ladestellen.at access:
    # ECONTROL_AT_APIKEY=<MY ECONTROL API KEY> # access via API key not working currently, see charging_stations_pipelines/pipelines/at/README.md
    # ECONTROL_AT_DOMAIN=<DOMAIN PROVIDED DURING REGISTRATION>

### Start docker containers

```bash
docker compose up # or: docker-compose up
```

### Initialize DB

Run

    alembic revision --autogenerate -m "create initial echarm tables"

Manually adapt generated db migration file in alembic/versions folder and add

    from geoalchemy2.types import Geometry, Geography

and remove prefix `geoalchemy2.types.` anywhere else in file

Then run migration

    alembic upgrade head

### Running eCharm

eCharm can be run similar to a command line tool.
Run `python main.py -h` to see the full list of command line options.
Run `python list-countries.py` to see a list of supported countries
together with information about the availability of a governmental data source, and availability in OSM and OCM.

Here are a few example commands for running tasks:

#### Import and merge stations for all available countries:

```bash
python main.py import merge --delete_data
```

Note that you have to configure API-keys for certain data sources, as explained in the
[set environment variables section](#set-environment-variables).

We also recommend to use the `--delete_data` flag to remove old data from the database before running import or merge
tasks, since eCharm is not (yet) clever with updating data from consecutive imports or merges.

#### Import and merge stations for Germany and Italy only:

```bash
python main.py import merge --countries de it --delete_data
```

Currently, we support the [ISO 3166-1 alpha 2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2) 
country codes of most European countries.

#### Export all original (un-merged) station data for Germany in csv format:

```bash
python main.py export --countries de
```

#### Export merged stations in a 10km radius around the Munich city center in GeoJSON format:

```bash
python main.py export --export_format GeoJSON --export_merged_stations --export_file_descriptor Munich --export_area 11.574774 48.1375526 10000.0
```

## Contributing

### Testing

Before you run the tests you need to install test dependencies by

```bash
pip install -r test/requirements.txt
```

You can run all tests by running the following command:

```bash
# to run all tests, use:
python -m pytest

# ... or the following to run only the unit tests, i.e. not the integration tests (which run a bit longer): 
python -m pytest -m 'not integration_test'
```

#### Testdata import / Integration test for the merger

For running the script `testdata_import.py` you need to be added as user to the API in Google Cloud
and the Spreadsheet needs to be shared with the same user.

### Changes to the data model

If you change the data model, please use alembic to create new revision by

```bash
alembic revision --autogenerate -m "your revision comment"
```

### Coding Style & Formatting

We are using 
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
for linting and code formatting.

A pre-commit hook can be set up with `pre-commit install` after installing dependencies.
