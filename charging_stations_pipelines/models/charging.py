from sqlalchemy import (
    ARRAY,
    Boolean,
    Column,
    Date,
    ForeignKey,
    Integer,
    String
)
from sqlalchemy.orm import relationship
from sqlalchemy.types import Float

from charging_stations_pipelines.models import Base
from charging_stations_pipelines.models.station import Station
from charging_stations_pipelines import settings


class Charging(Base):
    __tablename__ = f"{settings.db_table_prefix}charging"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer, ForeignKey(f"{Station.__tablename__}.id"), nullable=False, unique=True)
    date_created = Column(Date)
    date_updated = Column(Date)
    capacity = Column(Integer)
    kw_list = Column(ARRAY(Float, as_tuple=False))
    ampere_list = Column(ARRAY(Float, as_tuple=False))
    volt_list = Column(ARRAY(Float, as_tuple=False))
    socket_type_list = Column(ARRAY(String, as_tuple=False))
    dc_support = Column(Boolean)
    total_kw = Column(Float)
    max_kw = Column(Float)
    is_merged = Column(Boolean, default=False)
    station = relationship("Station", back_populates="charging")

    def __repr__(self):
        return "<charging with id: {}>".format(self.id)
