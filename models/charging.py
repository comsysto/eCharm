from sqlalchemy import ARRAY, Boolean, Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.types import Float

from models import Base


class Charging(Base):
    __tablename__ = "charging"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False, unique=True)
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
    station = relationship("Station", back_populates="charging")

    def __repr__(self):
        return "<charging with id: {}>".format(self.id)
