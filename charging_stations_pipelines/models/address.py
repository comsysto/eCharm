from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.types import Float

from charging_stations_pipelines.models import Base


class Address(Base):
    __tablename__ = "address"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False, unique=True)
    date_created = Column(Date)
    date_updated = Column(Date)
    street = Column(String)
    town = Column(String)
    district_old = Column(String)
    district = Column(String)
    state = Column(String)
    country = Column(String)
    gmaps_latitude = Column(Float(precision=32))
    gmaps_longitude = Column(Float(precision=32))
    station = relationship("Station", back_populates="address")

    def __repr__(self):
        return "<stations with id: {}>".format(self.id)
