# charging-stations-pipelines

## Table of contents
* [General info](#general-info)
* [Setup](#setup)
* [Contributing](#contributing)

## General info
`charging-stations-pipelines` can best be described as an electronic vehicle charging stations data integrator. 
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

#### Create enviroment and activate:
```
python -m venv venv
source venv/bin/activate  
```

#### Install the requirements file:
```
pip install -r requirements.txt  
```

### Set environment variables

Add

    DB_NAME=gis
    DB_HOST=localhost
    DB_PORT=54322
    DB_USER=docker
    DB_PASSWORD=docker

to `.env` file in the root directory

### Start docker containers
```bash
docker compose up  
```

### Initialize DB
```bash
alembic upgrade head
```

### Run your import/merge/export
```bash
python main.py --countries=de,it --tasks=import,merge,export --online=true
```
Feel free to adjust the command line arguments to your needs:
* `countries` Currently we support `de`,`gb`,`fr` and `it`
* `tasks`
  * `import` fetches and stores the data from the original sources, i.e. OSM, OCM and potential government data sources
  * `merge` searches for duplicates and merges attributes of duplicate stations
  * `export` create a data export for the specified countries in `csv` or `geo-json` format
* `online` fetch data online from original data sources, if `false` use files cached on disk

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
