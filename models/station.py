from geoalchemy2.types import Geometry
from sqlalchemy import Column, Date, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import relationship

from models import Base


class Station(Base):
    __tablename__ = "stations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String, index=True, nullable=False, unique=True)
    data_source = Column(String)
    operator = Column(String)
    payment = Column(String)
    authentication = Column(String)
    coordinates = Column(Geometry("POINT")) # TODO change to Geography column for exact distance measurement
    date_created = Column(Date)
    date_updated = Column(Date)
    raw_data = Column(String)
    country_code = Column(String)
    address = relationship("Address", back_populates="station", uselist=False)
    charging = relationship("Charging", back_populates="station", uselist=False)
    is_merged = Column(Boolean, default=False)


    def __repr__(self):
        return "<stations with id: {}>".format(self.id)

"""
class MergeRelation(Base):
    __tablename__ = "merge_relations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stations_id = Column(Integer, ForeignKey("stations.id"))


class StationMerged(Station):
    __tablename__ = "merged_stations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    # every merged station should have mapping to sources,
    # i.e. merge_relations 1:n mapping, maybe also with is_selected, is_duplicate otherwise extra columns for that in table?
    merge_relations = relationship("MergeRelation")

    __mapper_args__ = {
        "polymorphic_identity": "merged_stations",
        "concrete": True,
    }

"""