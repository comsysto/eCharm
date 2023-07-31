from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

from charging_stations_pipelines import settings

Base = declarative_base(metadata=MetaData(schema=settings.db_schema))

