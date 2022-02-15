from sqlalchemy import Column, DateTime, String, Integer, func, Date, Time, ForeignKey, ARRAY, Boolean
from sqlalchemy.orm import relationship
from geoalchemy2.types import Geometry

from models import Base
from sqlalchemy.types import Float
from enum import Enum
from sqlalchemy.dialects import postgresql


class Charging(Base):
    __tablename__ = 'charging'
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer, ForeignKey('stations.id'))
    date_created = Column(Date)
    date_updated = Column(Date)
    capacity = Column(Integer)
    kw_list = Column(ARRAY(Float))
    ampere_list = Column(ARRAY(Float))
    volt_list = Column(ARRAY(Float))
    socket_type_list = Column(ARRAY(String))
    dc_support = Column(Boolean)
    total_kw = Column(Float)
    max_kw = Column(Float)
    charging = relationship("Station")

    def __repr__(self):
        return '<charging with id: {}>'.format(self.id)
