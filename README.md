# stations-pipelines

## Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Setup](#setup)

## General info
This project is a refactoring project of da-charging.
	
## Technologies
Project is created with:
* Python 3.9
* Alembic
* Sqlalchemy 
* pandas
* etc...

# Coding Style & Formatting
Please take advantage of the following tooling:
```bash
pip install isort autoflake black
```

Black reformats the code, isort orders the imports and flake8 checks for remaining issues.
Example usage:
```bash
isort -rc -sl .
autoflake --remove-all-unused-imports -i -r .
isort -rc -m 3 .
```
	
## Setup
First create enviroment and activate:
```
python -m venv venv
source venv/bin/activate  
```

second install the requierements file:
```
pip install -r requirements.txt  
```

## docker
```bash
docker compose up  
```

```bash
alembic upgrade head
```


