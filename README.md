# eCharm: Electric Vehicle Charging Map

## Table of contents
* [General info](#general-info)
* [Setup](#setup)
* [Contributing](#contributing)

## General info
`eCharm` stands for Electric Vehicle Charging Map and can best be described as an electronic vehicle charging stations data integrator. 
It downloads data from different publicly available sources, converts it into a common format, 
searches for duplicates in the different sources and merges the data (e.g. the attributes). 
All steps are decoupled, so it's easy to integrate your own data source.

### Data Sources
The primary data sources for electronic vehicle charging stations that are generally available across Europe are 
Open Street Map (OSM) and Open Charge Map (OCM). 

For each country, we then integrate the official government data source (where available). 
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

    pyenv install 3.9.10
    pyenv local 3.9.10

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

#### Set environment variables
The following configuration parameters need to be set when using eCharm:

* DB_NAME: Specifies the name of the database to connect to.
* DB_HOST: Specifies the hostname or IP address of the database server.
* DB_PORT: Specifies the port number on which the database server is listening.
* DB_USER: Specifies the username to use when authenticating with the database server.
* DB_PASSWORD: Specifies the password to use when authenticating with the database server.
* DB_SCHEMA: Optionally possible to define schema where tables should be created. If not set it defaults to public.
* DB_TABLE_PREFIX: Optionally possible to define prefix for table names (e.g. `echarm_` would result in a table `echarm_stations`)
* DB_ALEMBIC_RESTRICT_TABLES: Optionally possible to set to `true` (default `false`), i.e. it only considers eCharm tables when checking for DB schema changes
* NOBIL_APIKEY: Specifies the API key required for accessing the NOBIL API. The NOBIL API is used to retrieve data from Sweden and Norway

##### Example
Put the following content to `.env` file in the root directory:

    DB_NAME=gis
    DB_HOST=localhost
    DB_PORT=54322
    DB_USER=docker
    DB_PASSWORD=docker
    # optional for DB:
    DB_SCHEMA=friendly_fox
    DB_TABLE_PREFIX=echarm_
    DB_ALEMBIC_RESTRICT_TABLES=true
    # for Nobil access:
    NOBIL_APIKEY=<MY_NOBIL_API_KEY>


### Start docker containers
```bash
docker compose up  
```

### Initialize DB

Run
    
    alembic revision --autogenerate -m "create initial echarm tables"

Manually adapt generated db migration file in alembic/versions folder and add

    from geoalchemy2.types import Geometry, Geography

and remove prefix `geoalchemy2.types.` anywhere else in file

Then run migration

    alembic upgrade head

### Run your import/merge/export
```bash
python main.py import merge export --countries de it
```

Run `python main.py -h` to see the full list of command line options.

Feel free to adjust the command line options to your needs:
* main tasks
  * `import` fetches and stores the data from the original sources, i.e. OSM, OCM and potential government data sources
  * `merge` searches for duplicates and merges attributes of duplicate stations
  * `export` create a data export for the specified countries in `csv` or `geo-json` format
* `countries` Currently we support `de`,`gb`,`fr`, `it`, `nor` and `swe`
* `offline` if not present (default), fetch data online from original data sources. If present, use files cached on disk
* `delete_data` if present: For import, delete all existing stations data before import. For merge, it delete only merged station data and reset merge status of original stations


## Contributing

### Testing

Before you run the tests you need to install test dependencies by 
```bash
pip install -r test/requirements.txt
```
You can run all tests under `/test` by running the following command:
```bash
python -m unittest discover test
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
Please take advantage of the following tooling:
```bash
pip install isort autoflake black
```

Black reformats the code, isort orders the imports and flake8 checks for remaining issues.
Example usage:
```bash
isort -rc -sl .
autoflake --remove-all-unused-imports -i -r --exclude alembic .
isort -rc -m 3 .
```
