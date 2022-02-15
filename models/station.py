from sqlalchemy import (
    ARRAY,
    BINARY,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Date,
)
from sqlalchemy.orm import relationship
from geoalchemy2.types import Geometry

from models import Base
from sqlalchemy.types import Float
from enum import Enum
from sqlalchemy.dialects import postgresql


class Station(Base):
    __tablename__ = 'stations'
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String, index=True)
    data_source = Column(String)
    operator = Column(String)
    payment = Column(String)
    authentication = Column(String)
    coordinates = Column(Geometry('POINT'))
    date_created = Column(Date)
    date_updated = Column(Date)
    raw_data = Column(String)
    address = relationship("Address")
    charging = relationship("Charging")


    def __repr__(self):
        return '<stations with id: {}>'.format(self.id)
