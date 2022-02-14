from sqlalchemy import Column, DateTime, String, Integer, func, Date, Time, ForeignKey
from sqlalchemy.orm import relationship
from geoalchemy2.types import Geometry

from models import Base
from sqlalchemy.types import Float
from enum import Enum
from sqlalchemy.dialects import postgresql


class Address(Base):
    __tablename__ = 'address'
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer, ForeignKey('stations.id'))
    date_created = Column(Date)
    date_updated = Column(Date)


    def __repr__(self):
        return '<stations with id: {}>'.format(self.id)
