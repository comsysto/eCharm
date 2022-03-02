from geoalchemy2.types import Geometry
from sqlalchemy import BigInteger, Column, Date, String, Integer
from sqlalchemy.orm import relationship

from models import Base


class Station(Base):
    __tablename__ = "stations"
    id = Column(Integer, primary_key=True)
    source_id = Column(String, index=True)
    data_source = Column(String)
    operator = Column(String)
    payment = Column(String)
    authentication = Column(String)
    coordinates = Column(Geometry("POINT"))
    date_created = Column(Date)
    date_updated = Column(Date)
    raw_data = Column(String)
    address = relationship("Address", back_populates="station", uselist=False)
    charging = relationship("Charging", back_populates="station", uselist=False)
    reference_hash = Column(String, nullable=False, unique=True)

    def __repr__(self):
        return "<stations with id: {}>".format(self.id)

    def __hash__(self):
        return hash(
            (
                self.data_source,
                self.operator,
                self.payment,
                self.authentication,
                self.coordinates,
                self.date_created,
                self.date_updated,
                self.raw_data,
                self.address,
                self.charging,
            )
        )
