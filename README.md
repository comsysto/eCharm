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
```
docker compose up  
```

```
alembic upgrade head
```
```
alembic downgrade -1
```


